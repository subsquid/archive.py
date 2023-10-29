import logging
from typing import Iterable

from sqa.util.log import init_logging
init_logging()

import sys

import pyarrow

import sqa.eth.ingest.writer
import sqa.substrate.writer.parquet
from sqa.fs import create_fs, Fs
from sqa.layout import get_chunks, DataChunk, get_chunks_in_reversed_order


LOG = logging.getLogger('sqa.rewrite')


def rewrite_archive(src: Fs, dest: Fs, chunks: Iterable[DataChunk]) -> None:
    for chunk in chunks:
        loc = src.cd(chunk.path())
        files = loc.ls()
        tables = {}
        for f in files:
            if f.endswith('.parquet'):
                table_name = f[:-len('.parquet')]
                tables[table_name] = _remove_non_data_columns(loc.read_parquet(f))

        if 'extrinsics.parquet' in files:
            sqa.substrate.writer.parquet.write_parquet(dest.cd(chunk.path()), tables)
        else:
            sqa.eth.ingest.writer.write_parquet(dest.cd(chunk.path()), tables)

        LOG.info('wrote %s', dest.abs(chunk.path()))


def _remove_non_data_columns(table: pyarrow.Table) -> pyarrow.Table:
    column_names = table.column_names
    for i in reversed(range(len(column_names))):
        name = column_names[i]
        if name == '_idx' or name.endswith('_size'):
            table = table.remove_column(i)
    return table


def _get_last_chunk(fs: Fs) -> DataChunk | None:
    for chunk in get_chunks_in_reversed_order(fs):
        if 'blocks.parquet' in fs.ls(chunk.path()):
            return chunk


def main():
    src = create_fs(sys.argv[1])
    dest = create_fs(sys.argv[2])
    if src.abs() == dest.abs():
        rewrite_archive(src, dest, get_chunks(src))
    else:
        last_chunk = _get_last_chunk(dest)
        first_block = last_chunk.last_block + 1 if last_chunk else 0
        rewrite_archive(src, dest, get_chunks(src, first_block=first_block))


if __name__ == '__main__':
    main()
