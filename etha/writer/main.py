import argparse
import os
import subprocess
import sys
from contextlib import contextmanager

from .writer import Writer
from ..fs import create_fs
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

    if args.src_node:
        _ingest(
            writer,
            args.src_node,
            chunk_writer.next_block,
            last_block=args.last_block,
            concurrency=args.src_node_concurrency
        )
    else:
        writer.write(sys.stdin)

    writer.flush()


def _ingest(writer, src_node, first_block, last_block=None, concurrency=None):
    cmd = ['squid-eth-ingest', '-e', src_node, '--from-block', str(first_block)]

    if last_block is not None:
        cmd.append('--to-block')
        cmd.append(str(last_block))

    if concurrency:
        cmd.append('--concurrency')
        cmd.append(str(concurrency))

    with _stdout(cmd) as lines:
        writer.write(lines)


@contextmanager
def _stdout(cmd: list[str]):
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE)
    try:
        yield proc.stdout
    except Exception as ex:
        proc.kill()
        raise ex
    proc.wait()


if __name__ == '__main__':
    main()

