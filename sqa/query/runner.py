import tempfile

import duckdb
import pyarrow

from sqa.fs import LocalFs
from sqa.layout import DataChunk, get_filelist
from sqa.query.model import Query
from sqa.query.sql import SqlQuery

CON = duckdb.connect(':memory:')


class QueryRunner:
    def __init__(self, dataset_dir: str, q: Query):
        self._dataset_dir = dataset_dir
        self._query = SqlQuery(q, get_filelist(LocalFs(dataset_dir)))
        # print(self._query.sql)

    def visit(self, chunk: DataChunk) -> pyarrow.ChunkedArray:
        self._query.set_chunk(self._dataset_dir, chunk)
        CON.execute(self._query.sql, self._query.params)
        return CON.fetch_arrow_table().column(0)


class ProfilingQueryRunner(QueryRunner):
    def __init__(self, dataset_dir: str, q: Query):
        super().__init__(dataset_dir=dataset_dir, q=q)
        self._exec_plans = []

    def visit(self, chunk: DataChunk) -> pyarrow.ChunkedArray:
        with tempfile.NamedTemporaryFile(mode='r', suffix='.json') as profile_out:
            CON.execute(f"PRAGMA enable_profiling='json'; PRAGMA profile_output='{profile_out.name}';")
            try:
                result = super().visit(chunk=chunk)
                self._exec_plans.append(profile_out.read())
                return result
            finally:
                CON.execute(f"PRAGMA disable_profiling;")

    @property
    def exec_plans(self) -> [str]:
        return self._exec_plans


def get_query_runner(dataset_dir: str, q: Query, profiling: bool = False) -> QueryRunner:
    if profiling:
        return ProfilingQueryRunner(dataset_dir=dataset_dir, q=q)
    return QueryRunner(dataset_dir=dataset_dir, q=q)
