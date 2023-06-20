import tempfile
from typing import Optional

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

    def visit(self, chunk: DataChunk, profiling: bool = False) -> (pyarrow.ChunkedArray, Optional[str]):
        self._query.set_chunk(self._dataset_dir, chunk)

        if profiling:
            with tempfile.NamedTemporaryFile(mode='r', suffix='.json', delete=False) as profile_out:
                CON.execute(f"PRAGMA enable_profiling='json'; PRAGMA profile_output='{profile_out.name}';")
                CON.execute(self._query.sql, self._query.params)
                result = CON.fetch_arrow_table().column(0)
                CON.execute(f"PRAGMA disable_profiling;")
                profile_json = profile_out.read()
                return result, profile_json
        else:
            CON.execute(self._query.sql, self._query.params)
            result = CON.fetch_arrow_table().column(0)
            return result, None

