import argparse
import asyncio
import logging
import multiprocessing as mp
import os
import sys

from etha.counters import Progress
from etha.fs import create_fs
from etha.ingest.ingest import Ingest
from etha.ingest.model import Block
from etha.ingest.rpc import RpcClient, RpcEndpoint
from etha.ingest.tables import qty2int
from etha.ingest.writer import Writer, BatchBuilder
from etha.layout import ChunkWriter
from etha.util import run_async_program, init_child_process, create_child_task, monitor_pipeline


LOG = logging.getLogger('etha.ingest.main')


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


async def main(args):
    endpoints = [RpcEndpoint(**e) for e in args.endpoints]

    rpc = RpcClient(
        endpoints=endpoints,
        batch_limit=args.batch_limit
    )

    fs = create_fs(args.dest or '.', s3_endpoint=args.s3_endpoint)

    chunk_writer = ChunkWriter(fs, first_block=args.first_block, last_block=args.last_block)
    chunk_writer.verify_last_chunk('blocks.parquet')

    if args.get_next_block:
        print(chunk_writer.next_block)
        sys.exit(0)

    if chunk_writer.next_block > chunk_writer.last_block:
        sys.exit(0)

    writer = Writer(chunk_writer, with_traces=args.with_traces)

    ingest = Ingest(
        rpc=rpc,
        finality_offset=args.best_block_offset,
        from_block=chunk_writer.next_block,
        to_block=args.last_block,
        with_receipts=args.with_receipts,
        with_traces=args.with_traces
    )

    await _Process(ingest, writer).run()


class _Process:
    def __init__(self, ingest: Ingest, writer: Writer):
        self._running = False
        self._ingest = ingest
        # FIXME: the queue size below is never a correct one.
        #   What is more, memory usage disparity is huge!
        self._write_queue = mp.Queue(10_000)
        self._write_process = mp.Process(
            target=_write_loop,
            args=(writer, self._write_queue),
            name='writer'
        )
        self._progress = Progress(window_size=10, window_granularity_seconds=1)
        self._progress.set_current_value(writer.chunk_writer.next_block)

    async def _ingest_loop(self):
        async for blocks in self._ingest.loop():
            self._write_queue.put(blocks)
            last_block = qty2int(blocks[-1]['number'])
            self._progress.set_current_value(last_block)

    async def _report_loop(self):
        while True:
            if self._progress.has_news():
                LOG.info(', '.join([
                    f'last block: {self._progress.get_current_value()}',
                    f'progress: {round(self._progress.speed())} blocks/sec'
                ]))
            await asyncio.sleep(5)

    async def _write_process_poll_loop(self):
        self._write_process.start()
        while True:
            try:
                await asyncio.sleep(1)
            except:
                self._write_process.kill()
                self._write_process.join()
                return
            if self._write_process.is_alive():
                pass
            else:
                self._write_process.join()
                if self._write_process.exitcode == 0:
                    return
                else:
                    raise Exception(
                        f'write process unexpectedly terminated with exit code {self._write_process.exitcode}'
                    )

    async def run(self):
        assert not self._running
        self._running = True
        ingest_task = create_child_task('ingest', self._ingest_loop())
        write_monitor_task = create_child_task('write_monitor', self._write_process_poll_loop())
        report_task = create_child_task('report', self._report_loop())
        try:
            await monitor_pipeline([ingest_task, write_monitor_task], service_task=report_task, log=LOG)
        finally:
            self._write_process.close()
            self._write_queue.close()


def _write_loop(writer: Writer, write_queue: mp.Queue) -> None:
    init_child_process()
    bb = BatchBuilder()
    while True:
        blocks: list[Block] = write_queue.get()
        if not blocks:
            return

        for b in blocks:
            bb.append(b)

        if bb.buffered_bytes() > 1024 * 1024 * 1024:
            batch = bb.build()
            writer.write(batch)


if __name__ == '__main__':
    run_async_program(main, parse_cli_arguments(), log=LOG)
