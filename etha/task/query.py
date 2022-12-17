import asyncio

import duckdb

from . import ipfs_service
from ..query.engine import QueryRunner
from ..query.model import Query
from ..query.result_set import ResultSet


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
