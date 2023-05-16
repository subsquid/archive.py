import logging
import os.path
from typing import Optional

from etha.worker.state.controller import RangeLock, StateController, State
from etha.worker.state.dataset import Dataset, dataset_encode
from etha.worker.state.sync import SyncProcess


LOG = logging.getLogger(__name__)


class StateManager:
    def __init__(self, data_dir: str):
        self._data_dir = data_dir
        self._sync = SyncProcess(data_dir)
        self._controller = StateController(self._sync)
        self._is_started = False

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
