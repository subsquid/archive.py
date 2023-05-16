import abc
import asyncio
import logging
from typing import AsyncIterator

import httpx

from etha.worker.state.controller import State
from etha.worker.state.intervals import to_range_set

LOG = logging.getLogger(__name__)


class Transport(abc.ABC):

    @abc.abstractmethod
    async def send_ping(self, state: State, pause=False) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    def state_updates(self) -> AsyncIterator[State]:
        raise NotImplementedError


class HttpTransport(Transport):
    def __init__(self,  worker_id: str, worker_url: str, router_url: str):
        self._worker_id = worker_id
        self._worker_url = worker_url
        self._router_url = router_url
        self._state_updates = asyncio.Queue(maxsize=100)

    async def send_ping(self, state: State, pause=False) -> None:
        ping_msg = {
            'worker_id': self._worker_id,
            'worker_url': self._worker_url + '/query',
            'state': state,
            'pause': pause,
        }

        async with httpx.AsyncClient(base_url=self._router_url) as client:
            try:
                response = await client.post('/ping', json=ping_msg)
                response.raise_for_status()
                result = response.json()
                desired_state = {
                    ds: to_range_set(map(tuple, ranges)) for ds, ranges in result.items()
                }
                await self._state_updates.put(desired_state)
            except httpx.HTTPError:
                LOG.exception('failed to send a ping message')

    async def state_updates(self) -> AsyncIterator[State]:
        while True:
            yield await self._state_updates.get()
