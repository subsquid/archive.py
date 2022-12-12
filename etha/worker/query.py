import datetime
import os
import tempfile
import zipfile
from contextlib import AbstractContextManager, contextmanager
from typing import NamedTuple, Optional

import duckdb
import pyarrow

from .intervals import Range
from ..fs import LocalFs
from ..layout import get_chunks
from ..query.engine import QueryRunner
from ..query.model import Query

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
        if rs.size:
            filename, filesize = rs.save(self._out_dir)
            self.filename = filename
            self.filesize = filesize


class ResultSet:
    def __init__(self):
        self.size = 0
        self._tables = {}

    def write_blocks(self, table: Optional[pyarrow.Table]):
        self._write('blocks', table)

    def write_transactions(self, table: Optional[pyarrow.Table]):
        self._write('transactions', table)

    def write_logs(self, table: Optional[pyarrow.Table]):
        self._write('logs', table)

    def _write(self, name: str, table: Optional[pyarrow.Table]):
        if not table or table.shape[0] == 0:
            return

        batches = self._tables.get(name)
        if not batches:
            batches = []
            self._tables[name] = batches

        batches.append(table)
        self.size += table.get_total_buffer_size()

    def save(self, out_dir: Optional[str] = None) -> Optional[tuple[str, int]]:
        if self.size == 0:
            return None

        os.makedirs(out_dir, exist_ok=True)
        fd, filename = tempfile.mkstemp(dir=out_dir, prefix='result-', suffix='.zip')
        try:
            with os.fdopen(fd, 'wb') as f, \
                    zipfile.ZipFile(f, 'w', compression=zipfile.ZIP_DEFLATED, compresslevel=6) as arch:

                for name, batches in self._tables.items():
                    with arch.open(f'{name}.arrow', 'w') as zf:
                        stream = pyarrow.ipc.new_stream(zf, batches[0].schema)
                        for t in batches:
                            stream.write_table(t)
                        stream.close()

        except Exception as ex:
            os.unlink(filename)
            raise ex

        stat = os.stat(filename)
        return filename, stat.st_size
