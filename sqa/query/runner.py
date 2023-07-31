import tempfile

import duckdb
import pyarrow

from sqa.layout import DataChunk
from . import SqlQuery


CON = duckdb.connect(':memory:')


class QueryRunner:
    def __init__(self, dataset_dir: str, query: SqlQuery, profiling: bool = False):
        self._dataset_dir = dataset_dir
        self._query = query
        self._profile = profiling
        self.exec_plans = [] if profiling else None

    def visit(self, chunk: DataChunk) -> pyarrow.ChunkedArray:
        if self._profile:
            return self._profile_and_visit(chunk)
        else:
            return self._visit(chunk)

    def _visit(self, chunk: DataChunk) -> pyarrow.ChunkedArray:
        self._query.set_chunk(self._dataset_dir, chunk)
        CON.execute(self._query.sql, self._query.params)
        return CON.fetch_arrow_table().column(0)

    def _profile_and_visit(self, chunk: DataChunk) -> pyarrow.ChunkedArray:
        with tempfile.NamedTemporaryFile(mode='r', suffix='.json') as profile_out:
            CON.execute(f"PRAGMA enable_profiling='json'; PRAGMA profile_output='{profile_out.name}';")
            try:
                result = self._visit(chunk)
                self.exec_plans.append(profile_out.read())
                return result
            finally:
                CON.execute(f"PRAGMA disable_profiling;")
