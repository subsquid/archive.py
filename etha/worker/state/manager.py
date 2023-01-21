import asyncio
import logging
import os.path
from typing import Optional

import httpx

from etha.util import create_child_task, monitor_service_tasks
from etha.worker.state.controller import RangeLock, StateController
from etha.worker.state.dataset import Dataset, dataset_encode
from etha.worker.state.intervals import to_range_set
from etha.worker.state.sync import SyncProcess


LOG = logging.getLogger(__name__)


class StateManager:
    def __init__(self, data_dir: str, worker_id: str, worker_url: str, router_url: str):
        self._data_dir = data_dir
        self._worker_id = worker_id
        self._worker_url = worker_url
        self._router_url = router_url
        self._sync = SyncProcess(data_dir)
        self._controller = StateController(self._sync)
        self._is_started = False

    def get_dataset_dir(self, dataset: Dataset) -> str:
        return os.path.join(self._data_dir, dataset_encode(dataset))

    def get_temp_dir(self):
        return os.path.join(self._data_dir, 'temp')

    def use_range(self, dataset: Dataset, first_block: int) -> Optional[RangeLock]:
        return self._controller.use_range(dataset, first_block)

    def get_ping_message(self):
        return {
            'worker_id': self._worker_id,
            'worker_url': self._worker_url,
            'state': self._controller.get_state()
        }

    def get_status(self):
        return {
            'worker_id': self._worker_id,
            'worker_url': self._worker_url,
            'state': self._controller.get_status()
        }

    async def _ping_loop(self):
        async with httpx.AsyncClient(base_url=self._router_url) as client:
            try:
                while True:
                    await self._ping(client)
                    await asyncio.sleep(10)
            finally:
                await self._pause_ping(client)

    async def _ping(self, client: httpx.AsyncClient):
        try:
            response = await client.post('/ping', json=self.get_ping_message())
            response.raise_for_status()
        except httpx.HTTPError:
            LOG.exception('failed to send a ping message')
            return

        ping = response.json()
        LOG.info('ping', extra=ping)

        desired_state = {
            ds: to_range_set(map(tuple, ranges)) for ds, ranges in ping.items()
        }

        self._controller.ping(desired_state)

    async def _pause_ping(self, client: httpx.AsyncClient):
        msg = self.get_ping_message()
        msg['pause'] = True
        try:
            await client.post('/ping', json=msg, timeout=1)
        except:
            LOG.exception('failed to send a pause ping')

    async def run(self):
        assert not self._is_started
        self._is_started = True
        try:
            data_sync_task = create_child_task('data_sync', self._sync.run())
            ping_task = create_child_task('ping', self._ping_loop())
            await monitor_service_tasks([data_sync_task, ping_task], log=LOG)
        finally:
            self._sync.close()
