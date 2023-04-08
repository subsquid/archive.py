import asyncio
import multiprocessing.pool as mpl
from typing import Optional, Iterable

import falcon
import falcon.asgi as fa
import marshmallow as mm
import pyarrow

from etha.query.model import Query, query_schema
from etha.worker.query import execute_query
from etha.worker.state.dataset import dataset_decode, Dataset
from etha.worker.state.intervals import Range
from etha.worker.state.manager import StateManager


def max_body(limit: int):
    async def hook(req: fa.Request, *args):
        length = req.content_length

        if length is None:
            raise falcon.HTTPMissingHeader('content-length')

        if length > limit:
            msg = (
                    'The size of the request is too large. The body must not '
                    'exceed ' + str(limit) + ' bytes in length.'
            )
            raise falcon.HTTPPayloadTooLarge(
                title='Request body is too large', description=msg
            )

    return hook


async def get_json(req: fa.Request, schema: Optional[mm.Schema] = None):
    if req.content_type and req.content_type.startswith('application/json'):
        obj = await req.get_media()
        if not schema:
            return obj
        try:
            return schema.load(obj)
        except mm.ValidationError as err:
            raise falcon.HTTPBadRequest(description=f'validation error: {err.normalized_messages()}')
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

    @falcon.before(max_body(4 * 1024 * 1024))
    async def on_post(self, req: fa.Request, res: fa.Response, dataset: str):
        try:
            dataset = dataset_decode(dataset)
        except ValueError:
            raise falcon.HTTPNotFound(description=f'failed to decode dataset: {dataset}')

        q: Query = await get_json(req, query_schema)

        first_block = q['fromBlock']
        last_block = q.get('toBlock')
        if last_block is not None and last_block < first_block:
            raise falcon.HTTPBadRequest(description=f'fromBlock={last_block} > toBlock={first_block}')

        data_range_lock = self.sm.use_range(dataset, first_block)
        if data_range_lock is None:
            raise falcon.HTTPBadRequest(description=f'data for block {first_block} is not available')

        with data_range_lock as data_range:
            res.text = await self.execute_query(dataset, data_range, q)

        res.content_type = 'application/json'

    def execute_query(self, dataset: Dataset, data_range: Range, q: Query) -> asyncio.Future[str]:
        args = self.sm.get_dataset_dir(dataset), data_range, q
        loop = asyncio.get_event_loop()
        future = loop.create_future()

        def on_done(res):
            loop.call_soon_threadsafe(future.set_result, res)

        def on_error(err):
            loop.call_soon_threadsafe(future.set_exception, err)

        self.pool.apply_async(
            execute_query,
            args=args,
            callback=on_done,
            error_callback=on_error
        )

        return future


def create_app(sm: StateManager, pool: mpl.Pool) -> fa.App:
    app = fa.App()
    app.add_route('/status', StatusResource(sm))
    app.add_route('/query/{dataset}', QueryResource(sm, pool))
    return app
