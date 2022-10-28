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
