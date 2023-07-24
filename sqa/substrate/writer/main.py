import argparse
import json
import logging
import sys
from typing import Iterable

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

    return program.parse_args()


class WriteOptions(ArchiveWriteOptions):
    def get_block_height(self, block: Block) -> int:
        return block['header']['height']

    def get_block_hash(self, block: Block) -> str:
        return block['header']['hash']

    def get_block_parent_hash(self, block: Block) -> str:
        return block['header']['parentHash']

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

    end_of_write = writer.write(
        ParquetSink(),
        read_blocks(options, writer.get_next_block())
    )

    if not end_of_write:
        sys.exit(1)


def read_blocks(options: WriteOptions, next_block: int) -> Iterable[list[Block]]:
    pack = []
    for line in sys.stdin:
        if line:
            block: Block = json.loads(line)
            height = options.get_block_height(block)
            if height > options.last_block:
                break
            elif next_block <= height:
                pack.append(block)
                if len(pack) >= 50:
                    yield pack
                    pack = []
    if pack:
        yield pack


def cli():
    main(parse_cli_arguments())
