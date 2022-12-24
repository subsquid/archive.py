import datetime
import os
import tempfile
import zipfile
from contextlib import AbstractContextManager, contextmanager
from io import BytesIO
from typing import NamedTuple, Optional, Union

import duckdb

from etha.fs import LocalFs
from etha.layout import get_chunks
from etha.query.engine import QueryRunner
from etha.query.model import Query
from etha.query.result_set import ResultSet
from etha.worker.state.intervals import Range

CON = duckdb.connect(':memory:')


class QueryResult(NamedTuple):
    last_processed_block: int
    data: Optional[Union[str, bytes]]  # either name of a temp file or raw response body
    size: int


def execute_query(out_dir: str, dataset_dir: str, data_range: Range, q: Query) -> QueryResult:
    first_block = max(data_range[0], q['fromBlock'])
    last_block = q.get('toBlock')
    if last_block is None:
        last_block = data_range[1]
    else:
        last_block = min(data_range[1], last_block)

    assert first_block <= last_block

    beg = datetime.datetime.now()
    runner = QueryRunner(CON, q)
    result = Result(out_dir)
    last_processed_block = None

    with result.write() as rs:
        for chunk in get_chunks(LocalFs(dataset_dir), first_block=first_block, last_block=last_block):
            blocks, txs, logs = runner.run(
                os.path.join(dataset_dir, chunk.path())
            )

            rs.write_blocks(blocks)
            rs.write_transactions(txs)
            rs.write_logs(logs)

            last_processed_block = min(chunk.last_block, last_block)

            if rs.size > 4 * 1024 * 1024:
                break

            if datetime.datetime.now() - beg > datetime.timedelta(seconds=2):
                break

    assert last_processed_block is not None
    return QueryResult(last_processed_block, result.data, result.size)


class Result:
    def __init__(self, out_dir: str):
        self._out_dir = out_dir
        self.data = None
        self.size = 0

    @contextmanager
    def write(self) -> AbstractContextManager['ResultSet']:
        rs = ResultSet()
        yield rs

        if rs.size == 0:
            return

        if rs.size > 8 * 1024 * 1024:
            os.makedirs(self._out_dir, exist_ok=True)
            fd, filename = tempfile.mkstemp(dir=self._out_dir, prefix='result-', suffix='.zip')
            try:
                with os.fdopen(fd, 'wb') as dest:
                    _write_zip(dest, rs)
            except Exception as ex:
                os.unlink(filename)
                raise ex
            stat = os.stat(filename)
            self.data = filename
            self.size = stat.st_size
        else:
            dest = BytesIO()
            _write_zip(dest, rs)
            self.data = dest.getvalue()
            self.size = len(self.data)


def _write_zip(dest, rs: ResultSet):
    with zipfile.ZipFile(dest, 'w') as arch:
        for name, data in rs.close().items():
            with arch.open(f'{name}.arrow.gz', 'w') as zf:
                zf.write(data)
