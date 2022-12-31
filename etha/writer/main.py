import argparse
import os
import sys
import asyncio
import logging
import json
from threading import Thread
from queue import Queue

from .writer import Writer
from .ingest import Ingest, IngestOptions
from .rpc import RpcClient
from .progress import Progress
from ..fs import create_fs
from ..layout import ChunkWriter
from ..log import init_logging

LOG = logging.getLogger(__name__)


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
        help='rpc api url of an ethereum node to fetch data from'
    )

    program.add_argument(
        '--src-node-concurrency',
        type=int,
        help='maximum number of pending data requests allowed'
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

    rpc = RpcClient(args.src_node)

    progress = Progress(window_size=10, window_granularity_seconds=1)
    writing = Progress(window_size=10, window_granularity_seconds=1)

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
