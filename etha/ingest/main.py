import argparse
import os
import sys
import asyncio
import logging
import json
from threading import Thread
from queue import Queue

from etha.ingest.writer import Writer
from etha.ingest.ingest import Ingest, IngestOptions
from etha.ingest.rpc import RpcClient, RpcEndpoint
from etha.ingest.progress import Progress
from etha.ingest.metrics import Metrics
from etha.fs import create_fs
from etha.layout import ChunkWriter
from etha.log import init_logging

LOG = logging.getLogger(__name__)


class SrcNodeAction(argparse.Action):
    def __call__(self, parser, namespace, values, option_string):
        endpoints = namespace.__dict__.setdefault('endpoints', [])
        endpoint = {'url': values}
        endpoints.append(endpoint)


class SrcNodeLimitAction(argparse.Action):
    def __call__(self, parser, namespace, values, option_string):
        if 'endpoints' not in namespace:
            raise argparse.ArgumentError(self, '"--src-node" must be specified before the limit param')
        endpoint = namespace.endpoints[-1]
        if 'limit' in endpoint:
            raise argparse.ArgumentError(self, f'"{endpoint["url"]}" already has specified limit')
        endpoint['limit'] = values


async def main():
    program = argparse.ArgumentParser(
        description='Subsquid eth archive writer'
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
        '--dest',
        metavar='PATH',
        help='target dir to write data to'
    )

    program.add_argument(
        '--get-next-block',
        action='store_true',
        help='check the stored data, print the next block to write and exit'
    )

    program.add_argument(
        '--s3-endpoint',
        default=os.environ.get('AWS_S3_ENDPOINT'),
        help='s3 api endpoint for s3:// destinations'
    )

    program.add_argument(
        '--src-node',
        action=SrcNodeAction,
        help='rpc api url of an ethereum node to fetch data from'
    )

    program.add_argument(
        '--src-node-limit',
        action=SrcNodeLimitAction,
        type=int,
        help='maximum number of requests to the rpc api per second'
    )

    program.add_argument(
        '--src-node-concurrency',
        type=int,
        help='maximum number of pending data requests allowed'
    )

    program.add_argument(
        '--prom-port',
        type=int,
        help='port to use for built-in prometheus metrics server'
    )

    args = program.parse_args()

    fs = create_fs(args.dest or '.', s3_endpoint=args.s3_endpoint)

    chunk_writer = ChunkWriter(fs, first_block=args.first_block, last_block=args.last_block)
    chunk_writer.verify_last_chunk('blocks.parquet')

    if args.get_next_block:
        print(chunk_writer.next_block)
        sys.exit(0)

    if chunk_writer.next_block > chunk_writer.last_block:
        sys.exit(0)

    writer = Writer(chunk_writer)

    endpoints = [RpcEndpoint(e['url'], e.get('limit')) for e in args.endpoints]
    rpc = RpcClient(endpoints)

    progress = Progress(window_size=10, window_granularity_seconds=1)
    progress.set_current_value(chunk_writer.next_block)
    writing = Progress(window_size=10, window_granularity_seconds=1)

    if args.prom_port is not None:
        metrics = Metrics()
        metrics.add_progress(progress)
        metrics.add_rpc_metrics(rpc)
        metrics.serve(args.prom_port)

    total_bytes = 0

    def report():
        if progress.has_news():
            LOG.info(', '.join([
                f'last block: {progress.get_current_value()}',
                f'progress: {round(progress.speed())} blocks/sec',
                f'writing: {round(writing.speed() / 1024)} kb/sec',
            ]))

    every_task = asyncio.create_task(every(5, report))

    queue = Queue()
    thread = Thread(target=writer_task, args=(queue, writer), daemon=True)
    thread.start()

    options = IngestOptions(rpc, chunk_writer.next_block, args.last_block, args.src_node_concurrency)
    async for blocks in Ingest.get_blocks(options):
        for block in blocks:
            data = json.dumps(block, separators=(',', ':'))
            total_bytes += len(data)
            queue.put(data)
        progress.set_current_value(blocks[-1]['header']['number'])
        writing.set_current_value(total_bytes)

    every_task.cancel()
    report()


async def every(time: int, fn):
    while True:
        try:
            fn()
        except Exception:
            sys.exit(1)
        await asyncio.sleep(time)


def writer_task(queue: Queue, writer: Writer):
    while True:
        data: str = queue.get()
        writer.append(data)


if __name__ == '__main__':
    init_logging()
    asyncio.run(main())
