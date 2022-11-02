import argparse
import math
import sys

from .writer import Writer
from ..fs import LocalFs
from ..layout import ChunkWriter


def main():
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
        default=math.inf,
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

    args = program.parse_args()

    fs = LocalFs(args.dest or '.')

    chunk_writer = ChunkWriter(fs, first_block=args.first_block, last_block=args.last_block)

    if args.get_next_block:
        print(chunk_writer.next_block)
        sys.exit(0)

    writer = Writer(chunk_writer)

    for line in sys.stdin:
        if line:
            writer.append(line)

    writer.flush()


if __name__ == '__main__':
    main()

