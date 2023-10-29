import concurrent.futures
from functools import cached_property
from typing import Union, Any

import pyarrow

from sqa.duckdb import execute_sql
from sqa.fs import Fs


class Column:
    def __init__(self, data_type: pyarrow.DataType, chunk_size=1000):
        self.type = data_type
        self.chunk_size = chunk_size
        self.chunks = []
        self.buf = []

    def append(self, val):
        self.buf.append(val)
        if len(self.buf) >= self.chunk_size:
            self._new_chunk()

    def _new_chunk(self):
        a = pyarrow.array(self.buf, type=self.type)
        self.chunks.append(a)
        self.buf.clear()

    def bytesize(self):
        return sum(c.nbytes for c in self.chunks)

    def build(self) -> Union[pyarrow.ChunkedArray, pyarrow.Array]:
        if self.buf:
            self._new_chunk()
        size = len(self.chunks)
        if size == 0:
            return pyarrow.array([], type=self.type)
        elif size == 1:
            return self.chunks[0]
        else:
            return pyarrow.chunked_array(self.chunks)

    def reset(self) -> None:
        self.chunks = []
        self.buf = []


class TableBuilder:
    @cached_property
    def _columns(self) -> dict[str, Column]:
        columns = {}
        for n, c in self.__dict__.items():
            if isinstance(c, Column):
                columns[n] = c
        return columns

    def to_table(self) -> pyarrow.Table:
        names = []
        arrays = []
        for n, c in self._columns.items():
            names.append(n)
            arrays.append(c.build())
        return pyarrow.table(arrays, names=names)

    def bytesize(self) -> int:
        return sum(c.bytesize() for c in self._columns.values())

    def reset(self) -> None:
        for c in self._columns.values():
            c.reset()


_WRITE_POOL = concurrent.futures.ThreadPoolExecutor(thread_name_prefix='parquet_writer')


class BaseParquetSink:
    @cached_property
    def _tables(self) -> dict[str, TableBuilder]:
        tables = {}
        for k, v in self.__dict__.items():
            if isinstance(v, TableBuilder):
                tables[k] = v
        return tables

    def buffered_bytes(self) -> int:
        return sum(t.bytesize() for t in self._tables.values())

    def push(self, block: Any) -> None:
        raise NotImplementedError()

    def _write(self, fs: Fs, tables: dict[str, pyarrow.Table]) -> None:
        raise NotImplementedError()

    def flush(self, fs: Fs) -> None:
        arrow_tables = {}
        for n, t in self._tables.items():
            arrow_tables[n] = t.to_table()

        self._submit_write(fs, arrow_tables)

        for t in self._tables.values():
            t.reset()

    def _submit_write(self, fs: Fs, tables: dict[str, pyarrow.Table]) -> None:
        self._wait_for_prev_write()
        self.__dict__['_prev_write'] = _WRITE_POOL.submit(self._write_task, fs, tables)

    def _write_task(self, fs: Fs, tables: dict[str, pyarrow.Table]) -> None:
        with fs.transact('.') as tmp:
            self._write(tmp, tables)

    def _wait_for_prev_write(self) -> None:
        if prev_write := self.__dict__.get('_prev_write'):
            prev_write.result()
            self.__dict__['_prev_write'] = None

    def end(self):
        self._wait_for_prev_write()


def add_size_column(table: pyarrow.Table, col: str) -> pyarrow.Table:
    sizes = execute_sql(f'SELECT coalesce(strlen("{col}")::int8, 0) FROM "table"')
    return table.append_column(f'{col}_size', sizes.column(0))


def _get_size(v):
    if v is None:
        return 0
    else:
        return len(v)


def add_index_column(table: pyarrow.Table) -> pyarrow.Table:
    index = pyarrow.array(
        range(0, table.shape[0]),
        type=pyarrow.int32()
    )
    return table.append_column('_idx', index)
