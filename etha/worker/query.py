import datetime
import os
import tempfile
import zipfile
from contextlib import AbstractContextManager, contextmanager
from typing import NamedTuple, Optional

import duckdb

from etha.worker.state.intervals import Range
from ..fs import LocalFs
from ..layout import get_chunks
from ..query.engine import QueryRunner
from ..query.model import Query
from ..query.result_set import ResultSet

CON = duckdb.connect(':memory:')


class QueryResult(NamedTuple):
    last_processed_block: int
    filename: Optional[str]
    filesize: int


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
    return QueryResult(last_processed_block, result.filename, result.filesize)


class Result:
    def __init__(self, out_dir: str):
        self._out_dir = out_dir
        self.filename = None
        self.filesize = 0

    @contextmanager
    def write(self) -> AbstractContextManager['ResultSet']:
        rs = ResultSet()
        yield rs
        if rs.size == 0:
            return

        os.makedirs(self._out_dir, exist_ok=True)
        fd, filename = tempfile.mkstemp(dir=self._out_dir, prefix='result-', suffix='.zip')
        try:
            with os.fdopen(fd, 'wb') as f, \
                    zipfile.ZipFile(f, 'w') as arch:
                for name, data in rs.close().items():
                    with arch.open(f'{name}.arrow.gz', 'w') as zf:
                        zf.write(data)

        except Exception as ex:
            os.unlink(filename)
            raise ex

        stat = os.stat(filename)
        self.filename = filename
        self.filesize = stat.st_size
