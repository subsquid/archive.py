from etha.log import init_logging
if __name__ == '__main__':
    init_logging()

import logging
import argparse
import math

import pyarrow
import pyarrow.parquet

from etha.fs import S3Fs, s3fs
from etha.ingest.writer import ParquetWriter, ArrowDataBatch
from etha.layout import get_chunks, ChunkWriter

LOG = logging.getLogger(__name__)


def chunk_check(filelist: list[str]) -> bool:
    return 'blocks.parquet' in filelist


class ParquetReader:
    def __init__(self, bucket: str, fs: s3fs.S3FileSystem):
        self._bucket = bucket
        self._fs = fs

    def read_table(self, *segments):
        source = '/'.join([self._bucket, *segments])
        return pyarrow.parquet.read_table(source, filesystem=self._fs)


def main():
    program = argparse.ArgumentParser(description='Migrate data from one bucket to another')

    program.add_argument('--source-bucket', required=True)
    program.add_argument('--source-access-key', required=True)
    program.add_argument('--source-secret-key', required=True)
    program.add_argument('--source-endpoint', required=True)

    program.add_argument('--destination-bucket', required=True)
    program.add_argument('--destination-access-key', required=True)
    program.add_argument('--destination-secret-key', required=True)
    program.add_argument('--destination-endpoint', required=True)

    program.add_argument('--first-block', type=int, default=0)
    program.add_argument('--last-block', type=int)

    args = program.parse_args()

    client_kwargs = {'endpoint_url': args.source_endpoint}
    config_kwargs = {'read_timeout': 120}
    ss3 = s3fs.S3FileSystem(key=args.source_access_key, secret=args.source_secret_key,
                            client_kwargs=client_kwargs, config_kwargs=config_kwargs)

    client_kwargs = {'endpoint_url': args.destination_endpoint}
    config_kwargs = {'read_timeout': 120}
    ds3 = s3fs.S3FileSystem(key=args.destination_access_key, secret=args.destination_secret_key,
                            client_kwargs=client_kwargs, config_kwargs=config_kwargs)

    sfs = S3Fs(ss3, args.source_bucket)
    dfs = S3Fs(ds3, args.destination_bucket)

    chunk_writer = ChunkWriter(dfs, chunk_check, args.first_block, args.last_block)
    block_writer = ParquetWriter(dfs, chunk_writer, with_traces=True, with_statediffs=True)
    next_block = chunk_writer.next_block
    last_block = args.last_block if args.last_block else math.inf
    LOG.info(f'next block is {next_block}, last_block is {last_block}')

    reader = ParquetReader(args.source_bucket, ss3)
    for chunk in get_chunks(sfs, next_block, last_block):
        LOG.info(f'processing chunk {chunk.first_block}-{chunk.last_block}')

        path = chunk.path()
        batch = ArrowDataBatch(
            blocks=reader.read_table(path, 'blocks.parquet'),
            transactions=reader.read_table(path, 'transactions.parquet'),
            logs=reader.read_table(path, 'logs.parquet'),
            traces=reader.read_table(path, 'traces.parquet'),
            statediffs=reader.read_table(path, 'statediffs.parquet'),
            bytesize=0
        )
        block_writer.write(batch)


if __name__ == '__main__':
    main()
