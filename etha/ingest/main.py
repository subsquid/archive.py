import argparse
import asyncio
import concurrent.futures
import logging
import os
from typing import Optional

from etha.util.asyncio import create_child_task, monitor_pipeline, run_async_program
from etha.util.counters import Progress
from etha.ingest.ingest import Ingest
from etha.ingest.rpc import RpcClient, RpcEndpoint
from etha.ingest.tables import qty2int
from etha.ingest.writer import BatchBuilder, WriteOptions, WriteService


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
        help='fetch and write EVM traces (in Parity format)'
    )

    program.add_argument(
        '--write-chunk-size',
        metavar='MB',
        type=int,
        default=1024,
        help='data chunk size in roughly estimated megabytes'
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


async def ingest(args):
    endpoints = [RpcEndpoint(**e) for e in args.endpoints]

    rpc = RpcClient(
        endpoints=endpoints,
        batch_limit=args.batch_limit
    )

    write_options = WriteOptions(
        dest=args.dest,
        s3_endpoint=args.s3_endpoint,
        chunk_size=args.write_chunk_size,
        first_block=args.first_block,
        last_block=args.last_block,
        with_traces=args.with_traces
    )

    write_service = WriteService(write_options)
    chunk_writer = write_service.chunk_writer()
    chunk_writer.verify_last_chunk('blocks.parquet')

    if args.get_next_block:
        print(chunk_writer.next_block)
        return

    if chunk_writer.next_block > chunk_writer.last_block:
        return

    ingest = Ingest(
        rpc=rpc,
        finality_offset=args.best_block_offset,
        from_block=chunk_writer.next_block,
        to_block=args.last_block,
        last_hash=chunk_writer.last_hash,
        with_receipts=args.with_receipts,
        with_traces=args.with_traces
    )

    await IngestionProcess(ingest, write_service).run()


class IngestionProcess:
    def __init__(self, ingest: Ingest, write_service: WriteService):
        self._running = False
        self._ingest = ingest
        self._write_service = write_service
        self._progress = Progress(window_size=10, window_granularity_seconds=1)
        self._progress.set_current_value(write_service.chunk_writer().next_block)

    async def _ingest_loop(self):
        bb = BatchBuilder()
        writer = self._write_service.block_writer()
        chunk_size = self._write_service.options.chunk_size

        with concurrent.futures.ThreadPoolExecutor(
                max_workers=1,
                thread_name_prefix='block_writer'
        ) as executor:
            prev_write: Optional[concurrent.futures.Future] = None

            async for blocks in self._ingest.loop():
                first_block = qty2int(blocks[0]['number'])
                last_block = qty2int(blocks[-1]['number'])
                extra = {'first_block': first_block, 'last_block': last_block}

                LOG.debug('got stride', extra=extra)

                for b in blocks:
                    bb.append(b)

                if bb.buffered_bytes() > chunk_size * 1024 * 1024:
                    batch = bb.build()

                    LOG.debug('time to write a chunk', extra=extra)
                    if prev_write:
                        prev_write.result()

                    prev_write = executor.submit(writer.write, batch)
                    LOG.debug('scheduled for writing', extra=extra)

                self._progress.set_current_value(last_block)

            if prev_write:
                prev_write.result()

    async def _report_loop(self):
        while True:
            if self._progress.has_news():
                LOG.info(', '.join([
                    f'last block: {self._progress.get_current_value()}',
                    f'progress: {round(self._progress.speed())} blocks/sec'
                ]))
            await asyncio.sleep(5)

    async def run(self):
        assert not self._running
        self._running = True
        ingest_task = create_child_task('ingest', self._ingest_loop())
        report_task = create_child_task('report', self._report_loop())
        await monitor_pipeline([ingest_task], service_task=report_task)


def cli():
    run_async_program(cli, parse_cli_arguments())
