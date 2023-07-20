import logging
from typing import Optional
import os

import falcon
import falcon.asgi as fa
import marshmallow as mm
import sentry_sdk
from sentry_sdk.integrations.falcon import FalconIntegration

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

    @falcon.before(max_body(4 * 1024 * 1024))
    async def on_post(self, req: fa.Request, res: fa.Response, dataset: str):
        query: Query = await get_json(req, query_schema)

        try:
            query_result = await self._worker.execute_query(query, dataset)
            res.text = query_result.result
        except (QueryError, DataIsNotAvailable) as e:
            raise falcon.HTTPBadRequest(description=str(e))
        except Exception as e:
            LOG.exception('server error')
            raise falcon.HTTPInternalServerError(description=str(e))

        res.content_type = 'application/json'


def create_app(sm: StateManager, worker: Worker) -> fa.App:
    if sentry_dsn := os.getenv('SENTRY_DSN'):
        sentry_sdk.init(
            dsn=sentry_dsn,
            integrations=[FalconIntegration()],
            traces_sample_rate=1.0
        )

    app = fa.App()
    app.add_route('/status', StatusResource(sm))
    app.add_route('/query/{dataset}', QueryResource(worker))
    return app
