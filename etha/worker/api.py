import logging
import random
import time
from typing import Optional

import falcon
import falcon.asgi as fa
import marshmallow as mm

from etha.query.model import Query, query_schema
from etha.query.sql import DataIsNotAvailable
from etha.worker.state.manager import StateManager
from etha.worker.worker import QueryError, Worker


LOG = logging.getLogger(__name__)


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
            if random.random() < 0.2:
                LOG.warning('malformed request', extra={
                    'query': obj,
                    'validation_error': err.normalized_messages()
                })
            raise falcon.HTTPBadRequest(description=f'validation error: {err.normalized_messages()}')
    else:
        raise falcon.HTTPUnsupportedMediaType(description='expected json body')


class StatusResource:
    def __init__(self, sm: StateManager):
        self.sm = sm

    async def on_get(self, req: fa.Request, res: fa.Response):
        res.media = self.sm.get_state()


class QueryResource:
    def __init__(self, worker: Worker):
        self._worker = worker
        self._pending_requests = 0
        self._max_pending_requests = worker.get_processes_count() * 5

    @falcon.before(max_body(4 * 1024 * 1024))
    async def on_post(self, req: fa.Request, res: fa.Response, dataset: str):
        self._assert_is_not_busy()

        query: Query = await get_json(req, query_schema)

        if random.random() < 0.05:
            LOG.info('archive query', extra={'query': query})

        self._assert_is_not_busy()

        start_time = time.time()
        self._pending_requests += 1
        try:
            query_result = await self._worker.execute_query(query, dataset)
            res.text = query_result.result
            res.content_type = 'application/json'
        except (QueryError, DataIsNotAvailable) as e:
            if random.random() < 0.2:
                LOG.warning('archive query error', exc_info=e, extra={
                    'query': query
                })
            raise falcon.HTTPBadRequest(description=str(e))
        finally:
            self._pending_requests -= 1

        end_time = time.time()
        if end_time - start_time > 10:
            LOG.warning('slow query', extra={
                'query': query
            })

    def _assert_is_not_busy(self) -> None:
        if self._pending_requests >= self._max_pending_requests:
            raise falcon.HTTPServiceUnavailable(description='server is too busy')


def create_app(sm: StateManager, worker: Worker) -> fa.App:
    app = fa.App()
    app.add_route('/status', StatusResource(sm))
    app.add_route('/query/{dataset}', QueryResource(worker))
    return app
