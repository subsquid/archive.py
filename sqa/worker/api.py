import logging

import falcon
import falcon.asgi as fa

from sqa.query.builder import MissingData
from sqa.worker.query import QueryError
from sqa.worker.state.manager import StateManager
from sqa.worker.worker import Worker


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


async def get_json(req: fa.Request):
    if req.content_type and req.content_type.startswith('application/json'):
        return await req.get_media()
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
        query = await get_json(req)
        try:
            query_result = await self._worker.execute_query(query, dataset)
            res.text = query_result.result
            res.content_type = 'application/json'
        except (QueryError, MissingData) as e:
            raise falcon.HTTPBadRequest(
                title=e.__class__.__name__,
                description=str(e)
            )


def create_app(sm: StateManager, worker: Worker) -> fa.App:
    app = fa.App()
    app.add_route('/status', StatusResource(sm))
    app.add_route('/query/{dataset}', QueryResource(worker))
    return app
