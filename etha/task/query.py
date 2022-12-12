import asyncio
from io import BytesIO
from typing import Optional

import duckdb
import pyarrow

from . import ipfs_service
from ..query.engine import QueryRunner
from ..query.model import Query


async def execute_query(q: Query, chunks: list[str]):
    runner = QueryRunner(con=duckdb.connect(':memory:'), q=q)

    chunk_paths = asyncio.Queue()
    results = asyncio.Queue(1)

    async def chunk_resolver():
        for chunk_cid in chunks:
            chunk_path = await ipfs_service.get_cache(chunk_cid)
            chunk_paths.put_nowait(chunk_path)

    async def query_runner():
        rs = ResultSet()
        rs_chunk_size = 0

        for i in range(0, len(chunks)):
            c = await chunk_paths.get()
            blocks, txs, logs = runner.run(c)
            rs.write_blocks(blocks)
            rs.write_transactions(txs)
            rs.write_logs(logs)

            rs_chunk_size += 1
            left = len(chunks) - i
            if rs.size > 4 * 1024 * 1024 and float(left) / rs_chunk_size > 0.2:
                await results.put(rs.close())
                rs = ResultSet()
                rs_chunk_size = 0

        if rs.size > 0:
            await results.put(rs.close())

        await results.put(None)

    async def serializer():
        no = 0
        while rs := await results.get():
            no += 1
            for table, out in rs.items():
                await ipfs_service.publish(f'{no:03}/{table}.arrow.gz', out)

    done, pending = await asyncio.wait(
        [
            asyncio.create_task(chunk_resolver(), name='chunk_resolver'),
            asyncio.create_task(query_runner(), name='query_runner'),
            asyncio.create_task(serializer(), name='serializer')
        ],
        return_when=asyncio.FIRST_EXCEPTION
    )

    for tsk in pending:
        tsk.cancel()

    for tsk in done:
        if tsk.exception():
            raise tsk.exception()

    assert len(done) == 3


class ResultSet:
    _inputs: dict[str, pyarrow.RecordBatchStreamWriter]
    _outputs: dict[str, 'BytesBuffer']

    def __init__(self):
        self._inputs = {}
        self._outputs = {}
        self.size = 0

    def write_blocks(self, table: Optional[pyarrow.Table]):
        self._write('blocks', table)

    def write_transactions(self, table: Optional[pyarrow.Table]):
        self._write('transactions', table)

    def write_logs(self, table: Optional[pyarrow.Table]):
        self._write('logs', table)

    def _write(self, name: str, table: Optional[pyarrow.Table]):
        if not table or table.shape[0] == 0:
            return

        inp = self._inputs.get(name)
        if not inp:
            out = BytesBuffer()
            inp = pyarrow.ipc.new_stream(pyarrow.CompressedOutputStream(out, 'gzip'), table.schema)
            self._inputs[name] = inp
            self._outputs[name] = out

        inp.write_table(table)
        self.size += table.get_total_buffer_size()

    def close(self) -> dict[str, bytes]:
        tables = {}
        for table, inp in self._inputs.items():
            inp.close()
            out = self._outputs[table]
            tables[table] = out.getvalue()
        return tables


class BytesBuffer(BytesIO):
    def close(self):
        pass
