from typing import Union

import pyarrow


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
        return sum((c.nbytes for c in self.chunks), 0)

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


class TableBuilder:
    def to_table(self) -> pyarrow.Table:
        arrays = []
        names = []
        for n, c in self.__dict__.items():
            if isinstance(c, Column):
                names.append(n)
                arrays.append(c.build())
        return pyarrow.table(arrays, names=names)

    def bytesize(self) -> int:
        size = 0
        for c in self.__dict__.values():
            if isinstance(c, Column):
                size += c.bytesize()
        return size
