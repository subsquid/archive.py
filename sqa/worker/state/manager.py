import asyncio
import logging
import os.path
from typing import Optional

from sqa.worker.state.controller import RangeLock, StateController, State
from sqa.worker.state.dataset import Dataset, dataset_encode
from sqa.worker.state.sync import SyncProcess

LOG = logging.getLogger(__name__)


class StateManager:
    def __init__(self, data_dir: str):
        self._data_dir = data_dir
        self._sync = SyncProcess(data_dir)
        self._controller = StateController(self._sync)
        self._is_started = False

    async def get_stored_bytes(self) -> int:
        def compute():
            stored_bytes = 0
            for root, _, files in os.walk(self._data_dir):
                for file in files:
                    path = os.path.join(root, file)
                    if os.path.isfile(path):
                        stored_bytes += os.path.getsize(path)
            return stored_bytes

        # The walk is quite heavy, so let's not block the thread
        return await asyncio.get_event_loop().run_in_executor(None, compute)

    def get_dataset_dir(self, dataset: Dataset) -> str:
        return os.path.join(self._data_dir, dataset_encode(dataset))

    def get_temp_dir(self):
        return os.path.join(self._data_dir, 'temp')

    def use_range(self, dataset: Dataset, first_block: int) -> Optional[RangeLock]:
        return self._controller.use_range(dataset, first_block)

    def get_state(self) -> State:
        return self._controller.get_state()

    def update_state(self, desired_state: State) -> None:
        self._controller.ping(desired_state)

    async def run(self):
        assert not self._is_started
        self._is_started = True
        try:
            await self._sync.run()
        finally:
            self._sync.close()
