import asyncio
import base64
import multiprocessing.pool as mpl

import aiofiles
import aiofiles.os
import falcon
import falcon.asgi as fa

from .query import execute_query, QueryResult
from .state.intervals import Range
from .state.manager import StateManager
from ..query.model import Query


def get_json(req: fa.Request):
    if req.content_type and req.content_type.startswith('application/json'):
        return req.get_media()
    else:
        raise falcon.HTTPUnsupportedMediaType(description='expected json body')


class StatusResource:
    def __init__(self, sm: StateManager):
        self.sm = sm

    async def on_get(self, req: fa.Request, res: fa.Response):
        res.media = self.sm.get_status()


class QueryResource:
    def __init__(self, sm: StateManager, pool: mpl.Pool):
        self.sm = sm
        self.pool = pool

    async def on_post(self, req: fa.Request, res: fa.Response, dataset: str):
        try:
            dataset = base64.urlsafe_b64decode(dataset).decode(encoding='utf-8')
        except:
            raise falcon.HTTPNotFound(description=f'failed to decode dataset: {dataset}')

        if dataset != self.sm.get_dataset():
            raise falcon.HTTPNotFound(description=f'dataset {dataset} is not available')

        q: Query = await get_json(req)

        first_block = q['fromBlock']
        last_block = q['toBlock']
        if last_block is not None and last_block < first_block:
            raise falcon.HTTPBadRequest(description=f'fromBlock={last_block} > toBlock={first_block}')

        data_range_lock = self.sm.use_range(dataset, first_block)
        if data_range_lock is None:
            raise falcon.HTTPBadRequest(description=f'data for block {first_block} is not available')

        with data_range_lock as data_range:
            result: QueryResult = await self.execute_query(q, data_range)

        if result.filename:
            stream = await aiofiles.open(result.filename, 'rb')
            await aiofiles.os.unlink(result.filename)
            res.set_header('content-type', 'application/zip')
            res.set_stream(stream, result.filesize)
        else:
            res.status = 204

        res.set_header('x-sqd-last-processed-block', str(result.last_processed_block))

    def execute_query(self, q: Query, data_range: Range):
        future = asyncio.get_event_loop().create_future()
        args = self.sm.get_temp_dir(), self.sm.get_dataset_dir(), data_range, q
        self.pool.apply_async(
            execute_query,
            args=args,
            callback=future.set_result,
            error_callback=future.set_exception
        )
        return future


def create_app(sm: StateManager, pool: mpl.Pool) -> fa.App:
    app = fa.App()
    app.add_route('/status', StatusResource(sm))
    app.add_route('/query/{dataset}', QueryResource(sm, pool))
    return app
