import json
import logging
import os
import random
import time

import falcon
import falcon.asgi as fa

from sqa.query import MissingData
from sqa.worker.query import InvalidQuery
from sqa.worker.state.dataset import dataset_decode
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


def get_squid_id(req: fa.Request) -> str | None:
    return req.get_header('x-squid-id')


async def get_json(req: fa.Request):
    if req.content_type and req.content_type.startswith('application/json'):
        return await req.get_media()
    else:
        raise falcon.HTTPUnsupportedMediaType(description='expected json body')


class Limit:
    def __init__(self, max_pending_requests: int):
        self.max_pending_requests = max_pending_requests
        self._pending_requests = 0

    def assert_not_busy(self) -> None:
        if self._pending_requests >= self.max_pending_requests:
            raise falcon.HTTPServiceUnavailable(description='server is too busy')

    def start_of_request(self) -> None:
        self._pending_requests += 1

    def end_of_request(self) -> None:
        self._pending_requests -= 1


class StatusResource:
    def __init__(self, sm: StateManager, router_url: str, worker_id: str, worker_url: str, limit: Limit):
        self.sm = sm
        self.router_url = router_url
        self.worker_id = worker_id
        self.worker_url = worker_url
        self.limit = limit

    async def on_get(self, req: fa.Request, res: fa.Response):
        self.limit.assert_not_busy()
        res.content_type = 'application/json'
        res.text = json.dumps({
            'router_url': self.router_url,
            'worker_id': self.worker_id,
            'worker_url': self.worker_url,
            'state': {
                'available': self.sm.get_state(),
                'downloading': self.sm.get_downloading()
            }
        }, indent=2)


class QueryResource:
    def __init__(self, worker: Worker, limit: Limit):
        self._worker = worker
        self._limit = limit

    @falcon.before(max_body(4 * 1024 * 1024))
    async def on_post(self, req: fa.Request, res: fa.Response, dataset: str):
        self._limit.assert_not_busy()

        try:
            dataset = dataset_decode(dataset)
        except ValueError:
            raise falcon.HTTPBadRequest(description=f'failed to decode dataset: {dataset}')

        query = await get_json(req)

        log_extra = {
            'query_dataset': dataset,
            'query': query,
            'squid': get_squid_id(req)
        }

        if self._is_sampling():
            LOG.info('query sample', extra=log_extra)

        profiling = req.params.get('profile') == 'true'

        self._limit.assert_not_busy()

        start_time = time.time()

        self._limit.start_of_request()
        try:
            query_result = await self._worker.execute_query(query, dataset, profiling=profiling)
            res.data = query_result.compressed_data
            res.content_type = 'application/json'
            res.set_header('content-encoding', 'gzip')
        except InvalidQuery as e:
            LOG.warning(f'invalid query: {e}', extra=log_extra)
            raise falcon.HTTPBadRequest(description=str(e))
        except MissingData as e:
            LOG.warning(f'missing data: {e}', extra=log_extra)
            raise falcon.HTTPBadRequest(description=str(e))
        except Exception as e:
            LOG.exception('failed to execute query', extra=log_extra)
            raise falcon.HTTPInternalServerError(description=str(e))
        finally:
            self._limit.end_of_request()

        end_time = time.time()
        duration = end_time - start_time

        if duration > 10:
            LOG.warning('slow query', extra={
                'query_time': duration,
                'query_exec_time': query_result.exec_time,
                **log_extra
            })
        elif not self._is_sampling() and random.random() < 0.05:
            LOG.info('query sample', extra={
                'query_time': duration,
                **log_extra
            })

    def _is_sampling(self) -> bool:
        return os.environ.get('SQA_SAMPLE_ALL_QUERIES') == 'true'
