import argparse
import asyncio
import logging
import threading
from collections.abc import AsyncIterator, Generator
from functools import cache
from queue import Queue

from sqa.eth.ingest.main import EndpointAction
from sqa.starknet.writer.ingest import IngestStarknet
from sqa.starknet.writer.model import WriterBlock
from sqa.starknet.writer.writer import ParquetWriter
from sqa.util.rpc.client import RpcClient
from sqa.util.rpc.connection import RpcEndpoint
from sqa.writer import Writer
from sqa.writer.cli import CLI

LOG = logging.getLogger(__name__)


async def rpc_ingest(rpc: RpcClient, first_block: int, last_block: int | None = None, **ingest_kwargs: dict[str, bool]) -> AsyncIterator[list[WriterBlock]]:
    LOG.info('Ingesting data via RPC')

    # TODO: ensure starknet finality for finality_confirmation arg
    ingest = IngestStarknet(
        rpc=rpc,
        from_block=first_block,
        to_block=last_block,
        **ingest_kwargs,
    )

    try:
        async for bb in ingest.loop():
            yield bb
    finally:
        ingest.close()


def _to_sync_gen(gen: AsyncIterator) -> Generator:
    q = Queue(maxsize=5)

    async def consume_gen() -> None:
        try:
            async for it in gen:
                q.put(it)
            q.put(None)
        except Exception as ex:  # noqa: BLE001
            q.put(ex)

    # FIXME: learn how to do that properly
    threading.Thread(target=lambda: asyncio.run(consume_gen())).start()

    while True:
        it = q.get()
        if isinstance(it, Exception):
            raise it
        elif it is None:
            return
        else:
            yield it


class _CLI(CLI):
    @cache
    def _arguments(self) -> argparse.Namespace:
        program = argparse.ArgumentParser(
            prog=f'python3 -m {self.module_name}',
            description='Subsquid substrate archive writer',
        )

        program.add_argument(
            'dest',
            metavar='ARCHIVE',
            help='target dir or s3 location to write data to',
        )

        program.add_argument(
            '-e', '--endpoint',
            action=EndpointAction,
            metavar='URL',
            required=True,
            default=[],
            help='rpc api url of an ethereum node to fetch data from',
        )

        program.add_argument(
            '--batch-limit',
            dest='batch_limit',
            metavar='N',
            type=int,
            default=200,
            help='maximum number of requests in RPC batch',
        )

        program.add_argument(
            '--first-block',
            type=int,
            default=0,
            metavar='N',
            help='first block of a range to write',
        )

        program.add_argument(
            '--last-block',
            type=int,
            metavar='N',
            help='last block of a range to write',
        )

        program.add_argument(
            '--chunk-size',
            metavar='MB',
            type=int,
            default=self.get_default_chunk_size(),
            help='data chunk size in roughly estimated megabytes',
        )

        program.add_argument(
            '--get-next-block',
            action='store_true',
            help='check the stored data, print the next block to write and exit',
        )

        program.add_argument(
            '--prom-port',
            type=int,
            help='port to use for built-in prometheus metrics server',
        )

        program.add_argument(
            '--with-traces',
            action='store_true',
            help='fetch and write starknet call traces',
        )

        program.add_argument(
            '--with-statediffs',
            action='store_true',
            help='fetch and write starknet state updates(diffs)',
        )

        return program.parse_args()

    def _ingest(self) -> Generator[list[WriterBlock], None, None]:  # type: ignore[return-value]  # NOTE: incompatible block type with dict type
        args = self._arguments()
        endpoints = [RpcEndpoint(**e) for e in args.endpoints]
        if not endpoints:
            raise RuntimeError('No endpoints provided')
        rpc = RpcClient(endpoints=endpoints, batch_limit=args.batch_limit)

        yield from _to_sync_gen(rpc_ingest(rpc, self._sink().get_next_block(), args.last_block, with_traces=args.with_traces, with_statediffs=args.with_statediffs))

    def create_writer(self) -> Writer:
        return ParquetWriter()


def main(module_name: str) -> None:
    _CLI(module_name).main()
