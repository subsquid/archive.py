import argparse
import json
import logging
import math
import os
import sys
import time
from typing import Iterable
from sqa.tron.writer.metrics import Metrics

from sqa.writer import ArchiveWriteOptions, ArchiveWriter
from .model import Block
from .parquet import ParquetSink


LOG = logging.getLogger(__name__)


def parse_cli_arguments():
    program = argparse.ArgumentParser(
        prog='python3 -m sqa.substrate.writer',
        description='Subsquid substrate archive writer'
    )

    program.add_argument(
        'dest',
        metavar='ARCHIVE',
        help='target dir or s3 location to write data to'
    )

    program.add_argument(
        '-s', '--src',
        type=str,
        metavar='URL',
        help='URL of the data ingestion service'
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
        default=1024,
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


class WriteOptions(ArchiveWriteOptions):
    def get_block_height(self, block: Block) -> int:
        return block['header']['height']

    def get_block_hash(self, block: Block) -> str:
        return block['header']['hash']

    def get_block_parent_hash(self, block: Block) -> str:
        return '0x' + block['header']['parentHash'][16:]

    def chunk_check(self, filelist: list[str]) -> bool:
        return 'blocks.parquet' in filelist


def main(args):
    options = WriteOptions(
        dest=args.dest,
        first_block=args.first_block,
        last_block=args.last_block,
        chunk_size=args.chunk_size
    )

    writer = ArchiveWriter(options)

    if args.get_next_block:
        print(writer.get_next_block())
        return

    if args.prom_port:
        metrics = Metrics()
        metrics.add_progress(writer._progress, writer._chunk_writer)
        metrics.serve(args.prom_port)

    if args.src:
        blocks = read_from_url(args.src, options, writer.get_next_block())
    else:
        blocks = read_from_stdin(options, writer.get_next_block())

    end_of_write = writer.write(
        ParquetSink(),
        batch(blocks)
    )

    if not end_of_write:
        sys.exit(1)


def batch(blocks: Iterable[Block]) -> Iterable[list[Block]]:
    pack = []
    for b in blocks:
        pack.append(b)
        if len(pack) >= 50:
            yield pack
            pack = []
    if pack:
        yield pack


def read_from_stdin(options: WriteOptions, next_block: int) -> Iterable[Block]:
    for line in sys.stdin:
        if line:
            block: Block = json.loads(line)
            height = options.get_block_height(block)
            if height > options.last_block:
                break
            elif next_block <= height:
                yield block


def read_from_url(url: str, options: WriteOptions, next_block: int) -> Iterable[Block]:
    import httpx

    data_range = {
        'from': next_block
    }
    if options.last_block < math.inf:
        data_range['to'] = options.last_block

    while data_range['from'] <= options.last_block:
        try:
            with httpx.stream('POST', url, json=data_range, timeout=httpx.Timeout(None)) as res:
                for line in res.iter_lines():
                    block: Block = json.loads(line)
                    height = options.get_block_height(block)
                    data_range['from'] = height + 1
                    yield block
        except (httpx.NetworkError, httpx.RemoteProtocolError):
            LOG.exception('data streaming error, will pause for 5 sec and try again')
            time.sleep(5)


def cli():
    if os.getenv('SENTRY_DSN'):
        import sentry_sdk
        sentry_sdk.init(
            traces_sample_rate=1.0
        )
    main(parse_cli_arguments())
