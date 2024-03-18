from functools import cached_property
import logging
import os
import concurrent.futures
import time
from typing import AsyncIterator, NamedTuple, Optional
from sqa.eth.ingest.main import WriteTask, parse_cli_arguments
from sqa.eth.ingest.metrics import Metrics
from sqa.fs import Fs, create_fs
from sqa.layout import ChunkWriter
from sqa.starknet.writer.ingest import IngestStarknet
from sqa.starknet.writer.model import Block
from sqa.starknet.writer.writer import ArrowBatchBuilder, ParquetWriter
from sqa.util.asyncio import run_async_program
from sqa.util.counters.progress import Progress
from sqa.util.rpc.client import RpcClient
from sqa.util.rpc.connection import RpcEndpoint

LOG = logging.getLogger(__name__)


async def run(args):
    write_options = WriteOptions(
        dest=args.dest,
        s3_endpoint=args.s3_endpoint,
        chunk_size=args.write_chunk_size,
        first_block=args.first_block,
        last_block=args.last_block,
        with_traces=args.with_traces,
        with_statediffs=args.with_statediffs,
        raw=args.raw
    )

    write_service = WriteService(write_options)

    if args.get_next_block:
        print(write_service.next_block())
        return

    if write_service.next_block() > write_service.last_block():
        return

    # init RPC client
    endpoints = [RpcEndpoint(**e) for e in args.endpoints]
    if endpoints:
        rpc = RpcClient(
            endpoints=endpoints,
            batch_limit=args.batch_limit
        )
    else:
        rpc = None

    # set up prometheus metrics
    if args.prom_port:
        metrics = Metrics()
        metrics.add_progress(write_service.progress, write_service._chunk_writer)
        if rpc:
            metrics.add_rpc_metrics(rpc)
        metrics.serve(args.prom_port)

    if args.raw_src:
        assert not args.raw, '--raw and --raw-src are mutually excluded args'
        raise NotImplementedError('Raw writer is not implemented for starknet')
    else:
        assert rpc, 'no endpoints were specified'
        strides = rpc_ingest(args, rpc, write_service.next_block(), write_service.last_block())

    await write_service.write_starknet(strides)

async def rpc_ingest(args, rpc: RpcClient, first_block: int, last_block: int | None = None) -> AsyncIterator[list[Block]]:
    LOG.info(f'ingesting data via RPC')

    ingest = IngestStarknet(
        rpc=rpc,
        finality_confirmation=args.best_block_offset,
        genesis_block=args.genesis_block,
        from_block=first_block,
        to_block=last_block,
        with_receipts=args.with_receipts,
        with_traces=args.with_traces,
        with_statediffs=args.with_statediffs,
        validate_tx_root=args.validate_tx_root,
        validate_tx_type=args.validate_tx_type
    )

    try:
        async for bb in ingest.loop():
            yield bb
    finally:
        ingest.close()


class StarknetBatch(NamedTuple):
    blocks: list[Block]
    extra: dict


class WriteOptions(NamedTuple):
    dest: str
    s3_endpoint: Optional[str] = None
    chunk_size: int = 1024
    first_block: int = 0
    last_block: Optional[int] = None
    with_traces: bool = False
    with_statediffs: bool = False
    raw: bool = False


class WriteService:
    def __init__(self, options: WriteOptions):
        self.options = options

    @cached_property
    def fs(self) -> Fs:
        return create_fs(self.options.dest, s3_endpoint=self.options.s3_endpoint)

    @cached_property
    def _chunk_writer(self) -> ChunkWriter:
        return ChunkWriter(
            self.fs,
            self._chunk_check,
            first_block=self.options.first_block,
            last_block=self.options.last_block
        )

    def _chunk_check(self, filelist: list[str]) -> bool:
        if self.options.raw:
            return 'blocks.jsonl.gz' in filelist
        else:
            return 'blocks.parquet' in filelist

    def next_block(self) -> int:
        return self._chunk_writer.next_block

    def last_block(self) -> int:
        return self._chunk_writer.last_block

    def last_hash(self) -> str | None:
        return self._chunk_writer.last_hash

    @cached_property
    def progress(self) -> Progress:
        progress = Progress(window_size=10, window_granularity_seconds=1)
        progress.set_current_value(self.next_block())
        return progress

    def report(self) -> None:
        LOG.info(
            f'last block: {self.progress.get_current_value()}, '
            f'progress: {round(self.progress.speed())} blocks/sec'
        )

    async def _batches_starknet(
            self,
            strides: AsyncIterator[list[WriterBlock]]
    ) -> AsyncIterator[StarknetBatch]:
        last_report = 0
        last_block_hash = self.last_hash()

        async for blocks in strides:
            # NOTE: block_number
            first_block = blocks[0]['number']  # StarkNet block numbers are direct integers
            last_block = blocks[-1]['number']
            extra = {'first_block': first_block, 'last_block': last_block}
            LOG.debug('got stride', extra=extra)

            # validate chain continuity
            for block in blocks:
                block_parent_hash = block['parent_hash']  # Assuming StarkNet blocks include a parent hash
                block_hash = block['block_hash']
                if last_block_hash and last_block_hash != block_parent_hash:
                    raise Exception(f'broken chain: block {block_hash} is not a direct child of {last_block_hash}')
                last_block_hash = block_hash

            yield StarknetBatch(blocks=blocks, extra=extra)

            current_time = time.time()
            self.progress.set_current_value(last_block, current_time)
            if current_time - last_report > 5:
                self.report()
                last_report = current_time

        if self.progress.has_news():
            self.report()

    async def write_starknet(self, strides: AsyncIterator[list[WriterBlock]]):
        batches = self._batches_starknet(strides)

        if self.options.raw:
            raise NotImplementedError('Raw writer is not supported for starknet')
        else:
            tasks = self._parquet_writer_starknet(batches)

        await self.submit_write_tasks(tasks)

    async def submit_write_tasks(self, tasks: AsyncIterator[WriteTask]):
        with concurrent.futures.ThreadPoolExecutor(
                max_workers=1,
                thread_name_prefix='block_writer'
        ) as executor:
            prev_write: Optional[concurrent.futures.Future] = None

            async for t in tasks:
                if prev_write:
                    prev_write.result()
                prev_write = executor.submit(*t)

            if prev_write:
                prev_write.result()

    async def _parquet_writer_starknet(self, batches: AsyncIterator[StarknetBatch]) -> AsyncIterator[WriteTask]:
        bb = ArrowBatchBuilder(self.options.with_traces, self.options.with_statediffs)
        chunk_size = self.options.chunk_size

        writer = ParquetWriter(
            self.fs,
            self._chunk_writer,
            with_traces=self.options.with_traces,
            with_statediffs=self.options.with_statediffs
        )

        async for blocks, extra in batches:
            for b in blocks:
                bb.append(b) # NOTE: ensure types

            if bb.buffered_bytes() > chunk_size * 1024 * 1024:
                LOG.debug('time to write a chunk', extra=extra)
                batch = bb.build() # NOTE: ensure types
                yield writer.write, batch

        if bb.buffered_bytes() > 0:
            batch = bb.build() # NOTE: ensure types
            LOG.debug('flush buffered data from last strides')
            writer.write(batch)


def cli():
    if os.getenv('SENTRY_DSN'):
        import sentry_sdk
        sentry_sdk.init(
            traces_sample_rate=1.0
        )
    run_async_program(run, parse_cli_arguments())
