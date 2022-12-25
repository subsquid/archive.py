import asyncio

from etha.query.model import Query
from etha.query.result_set import ResultSet
from etha.query.runner import QueryRunner
from etha.task import ipfs_service


async def execute_query(q: Query, chunks: list[str]):
    runner = QueryRunner(q)
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
            runner.visit(c, rs)
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
