import asyncio
import logging
from typing import Optional

import httpx

from etha.writer.speed import Speed

LOG = logging.getLogger(__name__)


class RetriableException(Exception):
    pass


class Connection:
    def __init__(self, url: str, limit: Optional[int], client: httpx.AsyncClient):
        self.url = url
        self.limit = limit
        self._client = client
        self._speed = Speed(window_size=200)
        self._online = True
        self._backoff_schedule = [10, 100, 500, 2000, 10000, 20000]
        self._errors_in_row = 0

    def __hash__(self):
        return hash(self.url)

    def average_response_time(self):
        return self._speed.time() or 0.01

    def is_online(self):
        return self._online

    async def request(self, data):
        try:
            response = await self._perform_request(data)
        except httpx.HTTPStatusError as e:
            if _is_retryable_error(e):
                self._backoff()
                raise RetriableException
            raise e
        self._errors_in_row = 0
        return response

    async def _perform_request(self, data):
        headers = {
            'accept': 'application/json',
            'accept-encoding': 'gzip, br',
            'content-type': 'application/json',
        }
        response = await self._client.post(self.url, json=data, headers=headers)
        response.raise_for_status()
        return response.json()

    def _backoff(self):
        backoff = self._backoff_schedule[min(self._errors_in_row, len(self._backoff_schedule) - 1)]
        LOG.warning(f'going offline for {backoff} ms')
        self._errors_in_row += 1
        self._online = False
        task = asyncio.create_task(asyncio.sleep(backoff / 1000))
        task.add_done_callback(lambda _: self._reconnect())

    def _reconnect(self):
        LOG.debug('online')
        self._online = True


def _is_retryable_error(e: httpx.HTTPStatusError) -> bool:
    return e.response.status_code in (429, 502, 503, 504)
