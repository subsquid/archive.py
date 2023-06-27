import argparse
import asyncio
import concurrent.futures
import gzip
import json
import logging
import math
import os
import tempfile
import time
from functools import cached_property
from typing import Optional, NamedTuple, AsyncIterator, Callable, Any

import pyarrow

from etha.fs import create_fs, Fs, LocalFs
from etha.ingest.ingest import Ingest
from etha.ingest.metrics import Metrics
from etha.ingest.model import Block
from etha.ingest.rpc import RpcClient, RpcEndpoint
from etha.ingest.tables import qty2int
from etha.ingest.util import short_hash
from etha.ingest.writer import ArrowBatchBuilder, ParquetWriter
from etha.layout import ChunkWriter, get_chunks, DataChunk
from etha.util.asyncio import run_async_program
from etha.util.counters import Progress


LOG = logging.getLogger(__name__)


class EndpointAction(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):
        endpoints = namespace.__dict__.setdefault('endpoints', [])
        endpoint = {'url': values}
        endpoints.append(endpoint)


class EndpointOptionAction(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):
        if 'endpoints' not in namespace:
            raise argparse.ArgumentError(
                self,
                f'"-e, --endpoint" option must be specified before "{option_string}"'
            )
        endpoint = namespace.endpoints[-1]
        endpoint[self.dest] = values


class EndpointListOptionAction(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):
        if 'endpoints' not in namespace:
            raise argparse.ArgumentError(
                self,
                f'"-e, --endpoint" option must be specified before "{option_string}"'
            )
        endpoint = namespace.endpoints[-1]
        if self.dest in endpoint:
            endpoint[self.dest].append(values)
        else:
            endpoint[self.dest] = [values]


def parse_cli_arguments():
    program = argparse.ArgumentParser(
        description='Subsquid eth archive ingest'
    )

    program.add_argument(
        '-e', '--endpoint',
        action=EndpointAction,
        metavar='URL',
        required=True,
        default=[],
        help='rpc api url of an ethereum node to fetch data from'
    )

    program.add_argument(
        '-c', '--endpoint-capacity',
        action=EndpointOptionAction,
        dest='capacity',
        metavar='N',
        type=int,
        help='maximum number of allowed pending requests'
    )

    program.add_argument(
        '-r', '--endpoint-rate-limit',
        action=EndpointOptionAction,
        dest='rps_limit',
        metavar='RPS',
        type=int,
        help='maximum number of requests per second'
    )

    program.add_argument(
        '--batch-limit',
        dest='batch_limit',
        metavar='N',
        type=int,
        default=200,
        help='maximum number of requests in RPC batch'
    )

    program.add_argument(
        '-m', '--endpoint-missing-method',
        action=EndpointListOptionAction,
        dest='missing_methods',
        metavar='NAME'
    )

    program.add_argument(
        '--dest',
        metavar='PATH',
        help='target dir to write data to'
    )

    program.add_argument(
        '--first-block',
        type=int,
        default=0,
        metavar='N',
        help='first block of a range to write'
    )

    program.add_argument(
        '--last-block',
        type=int,
        metavar='N',
        help='last block of a range to write'
    )

    program.add_argument(
        '--best-block-offset',
        type=int,
        metavar='N',
        default=30,
        help='finality offset from the head of a chain'
    )

    program.add_argument(
        '--with-receipts',
        action='store_true',
        help='fetch and write transaction receipt data'
    )

    program.add_argument(
        '--with-traces',
        action='store_true',
        help='fetch and write EVM call traces'
    )

    program.add_argument(
        '--with-statediffs',
        action='store_true',
        help='fetch and write EVM state updates'
    )

    program.add_argument(
        '--use-trace-api',
        action='store_true',
        help='use trace_* API for statediffs and call traces'
    )

    program.add_argument(
        '--use-debug-api-for-statediffs',
        action='store_true',
        help='use debug prestateTracer to fetch statediffs (by default will use trace_* api)'
    )

    program.add_argument(
        '--write-chunk-size',
        metavar='MB',
        type=int,
        default=1024,
        help='data chunk size in roughly estimated megabytes'
    )

    program.add_argument(
        '--raw',
        action='store_true',
        help='use raw .jsonl.gz format'
    )

    program.add_argument(
        '--raw-src',
        metavar='PATH',
        help='archive with raw, pre-fetched .jsonl.gz data'
    )

    program.add_argument(
        '--get-next-block',
        action='store_true',
        help='check the stored data, print the next block to write and exit'
    )

    program.add_argument(
        '--s3-endpoint',
        metavar='URL',
        default=os.environ.get('AWS_S3_ENDPOINT'),
        help='s3 api endpoint for s3:// destinations'
    )

    program.add_argument(
        '--prom-port',
        type=int,
        help='port to use for built-in prometheus metrics server'
    )

    return program.parse_args()


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
        strides = raw_ingest(args.raw_src, write_service.next_block(), write_service.last_block())
    else:
        assert rpc, 'no endpoints were specified'
        strides = rpc_ingest(args, rpc, write_service.next_block(), write_service.last_block())

    await write_service.write(strides)


async def rpc_ingest(args, rpc: RpcClient, first_block: int, last_block: int | None = None) -> AsyncIterator[list[Block]]:
    LOG.info(f'ingesting data via RPC')

    ingest = Ingest(
        rpc=rpc,
        finality_confirmation=args.best_block_offset,
        from_block=first_block,
        to_block=last_block,
        with_receipts=args.with_receipts,
        with_traces=args.with_traces,
        with_statediffs=args.with_statediffs,
        use_trace_api=args.use_trace_api,
        use_debug_api_for_statediffs=args.use_debug_api_for_statediffs,
    )

    try:
        async for bb in ingest.loop():
            yield bb
    finally:
        ingest.close()


async def raw_ingest(src: str, first_block: int = 0, last_block: int = math.inf) -> AsyncIterator[list[Block]]:
    LOG.info(f'ingesting pre-fetched data from {src}')
    fs = create_fs(src)
    loop = asyncio.get_event_loop()

    async for chunk in stream_chunks(fs, first_block, last_block):
        blocks = await loop.run_in_executor(None, load_chunk, fs, chunk, first_block, last_block)
        yield blocks


async def stream_chunks(fs: Fs, first_block: int = 0, last_block: int = math.inf) -> AsyncIterator[DataChunk]:
    while first_block <= last_block:
        pos = first_block

        for chunk in get_chunks(fs, first_block=first_block, last_block=last_block):
            yield chunk
            first_block = chunk.last_block + 1

        if pos == first_block:
            LOG.info('no chunks were found. waiting 5 min for a new try')
            await asyncio.sleep(5 * 60)


def load_chunk(fs: Fs, chunk: DataChunk, first_block: int, last_block: int) -> list[Block]:
    with fs.open(f'{chunk.path()}/blocks.jsonl.gz', 'rb') as f, gzip.open(f) as lines:
        blocks = []
        for line in lines:
            block: Block = json.loads(line)
            height = qty2int(block['number'])
            if first_block <= height <= last_block:
                blocks.append(block)
        return blocks


class Batch(NamedTuple):
    blocks: list[Block]
    extra: dict


WriteTask = tuple[Callable, Any, ...]


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

    async def _batches(
            self,
            strides: AsyncIterator[list[Block]]
    ) -> AsyncIterator[Batch]:
        last_report = 0
        last_hash = self.last_hash()

        async for blocks in strides:
            first_block = qty2int(blocks[0]['number'])
            last_block = qty2int(blocks[-1]['number'])
            extra = {'first_block': first_block, 'last_block': last_block}
            LOG.debug('got stride', extra=extra)

            # validate chain continuity
            for block in blocks:
                block_parent_hash = short_hash(block['parentHash'])
                block_hash = short_hash(block['hash'])
                if last_hash and last_hash != block_parent_hash:
                    raise Exception(f'broken chain: block {block_hash} is not a direct child of {last_hash}')
                last_hash = block_hash

            yield Batch(blocks=blocks, extra=extra)

            current_time = time.time()
            self.progress.set_current_value(last_block, current_time)
            if current_time - last_report > 5:
                self.report()
                last_report = current_time

        if self.progress.has_news():
            self.report()

    async def write(self, strides: AsyncIterator[list[Block]]):
        batches = self._batches(strides)
        if self.options.raw:
            tasks = self._raw_writer(batches)
        else:
            tasks = self._parquet_writer(batches)

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

    async def _raw_writer(self, batches: AsyncIterator[Batch]) -> AsyncIterator[WriteTask]:
        chunk_size = self.options.chunk_size
        while True:
            written = 0
            first_block = math.inf
            tmp = tempfile.NamedTemporaryFile(delete=False)
            try:
                with tmp, pyarrow.CompressedOutputStream(tmp, 'gzip') as out:
                    async for bb in batches:
                        first_block = min(first_block, qty2int(bb.blocks[0]['number']))
                        last_block = qty2int(bb.blocks[-1]['number'])
                        last_hash = bb.blocks[-1]['hash']

                        for block in bb.blocks:
                            line = json.dumps(block).encode('utf-8')
                            out.write(line)
                            out.write(b'\n')
                            written += len(line) + 1

                        if written > chunk_size * 1024 * 1024:
                            LOG.debug('time to write a chunk', extra=bb.extra)
                            break

                if written > 0:
                    chunk = self._chunk_writer.next_chunk(first_block, last_block, short_hash(last_hash))
                    dest = f'{chunk.path()}/blocks.jsonl.gz'
                    if isinstance(self.fs, LocalFs):
                        loc = self.fs.abs(dest)
                        os.makedirs(os.path.dirname(loc), exist_ok=True)
                        os.rename(tmp.name, loc)
                    else:
                        yield self._upload_temp_file, tmp.name, dest
                else:
                    return
            except:
                os.remove(tmp.name)
                raise

    def _upload_temp_file(self, tmp: str, dest: str):
        try:
            self.fs.upload(tmp, dest)
        finally:
            os.remove(tmp)

    async def _parquet_writer(self, batches: AsyncIterator[Batch]) -> AsyncIterator[WriteTask]:
        bb = ArrowBatchBuilder()
        chunk_size = self.options.chunk_size

        writer = ParquetWriter(
            self.fs,
            self._chunk_writer,
            with_traces=self.options.with_traces,
            with_statediffs=self.options.with_statediffs
        )

        async for blocks, extra in batches:
            for b in blocks:
                bb.append(b)

            if bb.buffered_bytes() > chunk_size * 1024 * 1024:
                LOG.debug('time to write a chunk', extra=extra)
                batch = bb.build()
                yield writer.write, batch

        if bb.buffered_bytes() > 0:
            batch = bb.build()
            LOG.debug('flush buffered data from last strides')
            writer.write(batch)


def cli():
    run_async_program(run, parse_cli_arguments())
