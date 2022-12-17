import asyncio
import logging
import os.path
from typing import Optional

import httpx

from .controller import RangeLock, State, StateController
from .intervals import to_range_set
from .sync import SyncProcess

LOG = logging.getLogger(__name__)


class StateManager:
    def __init__(self, data_dir: str, worker_id: str, worker_url: str, router_url: str):
        self._data_dir = data_dir
        self._worker_id = worker_id
        self._worker_url = worker_url
        self._router_url = router_url
        self._sync = SyncProcess(data_dir, self._download_callback)
        self._controller = StateController(self._sync)
        self._is_started = False

    def _download_callback(self, new_ranges: State) -> None:
        self._controller.download_callback(new_ranges)

    def get_dataset_dir(self) -> str:
        return os.path.join(self._data_dir, 'dataset')

    def get_temp_dir(self) -> str:
        return os.path.join(self._data_dir, 'temp')

    def get_dataset(self) -> str:
        return self._controller.get_dataset()

    def use_range(self, dataset: str, first_block: int) -> Optional[RangeLock]:
        return self._controller.use_range(dataset, first_block)

    def get_ping_message(self):
        msg = {
            'worker_id': self._worker_id,
            'worker_url': self._worker_url
        }
        if self._controller.get_dataset():
            msg['state'] = {
                'dataset': self._controller.get_dataset(),
                'ranges': [{'from': r[0], 'to': r[1]} for r in self._controller.get_available_ranges()]
            }
        return msg

    def get_status(self):
        return {
            'worker_id': self._worker_id,
            'worker_url': self._worker_url,
            'dataset': self._controller.get_dataset(),
            'available_ranges': self._controller.get_available_ranges(),
            'downloading_ranges': self._controller.get_downloading_ranges()
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
        desired_dataset = ping['dataset']
        desired_ranges = to_range_set((r['from'], r['to']) for r in ping['ranges'])
        desired_state = State(desired_dataset, desired_ranges)

        self._controller.ping(desired_state)

    async def _pause_ping(self, client: httpx.AsyncClient):
        msg = self.get_ping_message()
        msg['pause'] = True
        try:
            await client.post('/ping', json=msg, timeout=1)
        except:
            pass

    async def run(self):
        assert not self._is_started
        self._is_started = True
        try:
            sync_task = asyncio.create_task(self._sync.run(), name='sm:data_sync')
            ping_task = asyncio.create_task(self._ping_loop(), name='sm:ping')
            await _monitor_service_tasks(sync_task, ping_task)
        finally:
            self._sync.close()


async def _monitor_service_tasks(*tasks: asyncio.Task):
    # FIXME: validate this logic
    try:
        await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
    except asyncio.CancelledError as ex:
        for task in tasks:
            task.cancel()
        raise ex

    terminated = []

    for task in tasks:
        if task.done():
            terminated.append(task)
        else:
            task.cancel()

    tt = terminated[0]
    raise Exception(f'Service task {tt.get_name()} unexpectedly terminated') from tt.exception()
