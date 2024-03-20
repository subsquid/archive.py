import argparse
import asyncio
from functools import cache, cached_property
import logging
import os
import concurrent.futures
import time
from typing import AsyncIterator, Iterable, NamedTuple, Optional
from sqa.eth.ingest.main import EndpointAction, WriteTask, parse_cli_arguments
from sqa.eth.ingest.metrics import Metrics
from sqa.fs import Fs, create_fs
from sqa.layout import ChunkWriter
from sqa.starknet.writer.ingest import IngestStarknet
from sqa.starknet.writer.model import Block, WriterBlock
from sqa.starknet.writer.writer import ParquetWriter
from sqa.util.asyncio import run_async_program
from sqa.util.counters.progress import Progress
from sqa.util.rpc.client import RpcClient
from sqa.util.rpc.connection import RpcEndpoint
from sqa.writer import Writer
from sqa.writer.cli import CLI

LOG = logging.getLogger(__name__)


async def rpc_ingest(args, rpc: RpcClient, first_block: int, last_block: int | None = None) -> AsyncIterator[list[WriterBlock]]:
    LOG.info(f'ingesting data via RPC')

    # TODO: ensure starknet finality for finality_confirmation arg
    ingest = IngestStarknet(
        rpc=rpc,
        from_block=first_block,
        to_block=last_block
    )

    try:
        async for bb in ingest.loop():
            yield bb
    finally:
        ingest.close()


def sync_rpc_ingest(args, rpc: RpcClient, first_block: int, last_block: int | None = None) -> Iterable[list[WriterBlock]]:
    async def gather_blocks() -> list[list[WriterBlock]]:
        strides = []
        async for bb in rpc_ingest(args, rpc, first_block, last_block):
            strides.append(bb)
        return strides

    return asyncio.run(gather_blocks())

class _CLI(CLI):

    @cache
    def _arguments(self):
        program = argparse.ArgumentParser(
            prog=f'python3 -m {self.module_name}',
            description='Subsquid substrate archive writer'
        )

        program.add_argument(
            'dest',
            metavar='ARCHIVE',
            help='target dir or s3 location to write data to'
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
            '--batch-limit',
            dest='batch_limit',
            metavar='N',
            type=int,
            default=200,
            help='maximum number of requests in RPC batch'
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
            '--chunk-size',
            metavar='MB',
            type=int,
            default=self.get_default_chunk_size(),
            help='data chunk size in roughly estimated megabytes'
        )

        program.add_argument(
            '--get-next-block',
            action='store_true',
            help='check the stored data, print the next block to write and exit'
        )

        program.add_argument(
            '--prom-port',
            type=int,
            help='port to use for built-in prometheus metrics server'
        )

        return program.parse_args()

    def _ingest(self) -> Iterable[list[WriterBlock]]:  # type: ignore  # NOTE: incopatible block type with dict type
        args = self._arguments()
        endpoints = [RpcEndpoint(**e) for e in args.endpoints]
        if endpoints:
            rpc = RpcClient(
                endpoints=endpoints,
                batch_limit=args.batch_limit
            )
        else:
            rpc = None

        assert rpc, 'no endpoints were specified'
        strides = sync_rpc_ingest(args, rpc, args.first_block, args.last_block)

        return strides

    def create_writer(self) -> Writer:
        return ParquetWriter()


def main(module_name: str) -> None:
    _CLI(module_name).main()
