import duckdb
import pyarrow

from etha.fs import LocalFs
from etha.layout import DataChunk, get_filelist
from etha.query.model import Query
from etha.query.sql import SqlQuery


CON = duckdb.connect(':memory:')


class QueryRunner:
    def __init__(self, dataset_dir: str, q: Query):
        self._dataset_dir = dataset_dir
        self._query = SqlQuery(q, get_filelist(LocalFs(dataset_dir)))

    def visit(self, chunk: DataChunk) -> pyarrow.ChunkedArray:
        self._query.set_chunk(self._dataset_dir, chunk)
        CON.execute(self._query.sql, self._query.params)
        return CON.fetch_arrow_table().column(0)
