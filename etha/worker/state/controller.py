import bisect
import contextlib
from typing import NamedTuple, Optional, Protocol

from etha.worker.state.intervals import difference, Range, RangeSet, to_range_set, union


RangeLock = contextlib.AbstractContextManager[Range]


class State(NamedTuple):
    dataset: str
    ranges: RangeSet


class StateUpdate(NamedTuple):
    dataset: str
    new_ranges: RangeSet
    deleted_ranges: RangeSet


class Sync(Protocol):
    def send_update(self, upd: StateUpdate) -> None:
        pass

    def reset(self) -> State:
        pass


class StateController:
    def __init__(self, sync: Sync):
        state = sync.reset()
        self._sync = sync
        self._dataset = self._desired_dataset = state.dataset
        self._available = state.ranges
        self._downloading = []
        self._to_delete = []
        self._locked = {}

    def get_dataset(self) -> str:
        return self._dataset

    def get_available_ranges(self) -> list[Range]:
        return self._available

    def get_downloading_ranges(self) -> list[Range]:
        return self._downloading

    def get_range(self, first_block: int) -> Optional[Range]:
        if not self._available:
            return

        i = bisect.bisect_left(self._available, (first_block, first_block))

        if i < len(self._available):
            c = self._available[i]
            if c[0] == first_block:
                return c

        if i == 0:
            return

        c = self._available[i-1]
        if c[1] >= first_block:
            return c

    def use_range(self, dataset: str, first_block: int) -> Optional[RangeLock]:
        r = dataset == self._dataset == self._desired_dataset and self.get_range(first_block)
        if r:
            return self._range_lock(r)

    @contextlib.contextmanager
    def _range_lock(self, r: Range) -> RangeLock:
        self._locked[r] = self._locked.get(r, 0) + 1
        try:
            yield r
        finally:
            cnt = self._locked[r]
            if cnt > 1:
                self._locked[r] = cnt - 1
            else:
                del self._locked[r]

    def ping(self, desired_state: State) -> None:
        self._desired_dataset = desired_state.dataset

        if self._dataset == desired_state.dataset:
            new_dataset = False
        else:
            if self._locked:
                # someone still uses the current dataset
                # avoid any changes
                return
            else:
                self._reset(desired_state.dataset)
                new_dataset = True

        if difference(self._downloading, desired_state.ranges):
            # Some data ranges scheduled for downloading are not needed anymore.
            # This may be a sign of significant lag between the update process and the scheduler.
            # To eliminate the lag we restart the update process and check the current state
            # to issue a single update leading to the desired state.
            self._reset(desired_state.dataset)

        available = union(self._available, self._to_delete)

        new_ranges = difference(desired_state.ranges, union(available, self._downloading))
        deleted_ranges = difference(available, desired_state.ranges)

        locked = to_range_set(self._locked.keys())
        to_delete_now = difference(deleted_ranges, locked)

        self._to_delete = difference(deleted_ranges, to_delete_now)
        self._downloading = difference(desired_state.ranges, available)
        self._available = difference(desired_state.ranges, self._downloading)

        if new_dataset or new_ranges or to_delete_now:
            upd = StateUpdate(self._dataset, new_ranges=new_ranges, deleted_ranges=to_delete_now)
            self._sync.send_update(upd)

    def _reset(self, dataset: str):
        current_state = self._sync.reset()
        self._dataset = dataset
        self._downloading = []
        self._to_delete = []
        if current_state.dataset == dataset:
            self._available = current_state.ranges
        else:
            self._available = []

    def download_callback(self, new_ranges: State) -> None:
        assert self._dataset == new_ranges.dataset
        downloaded = new_ranges.ranges
        left_to_download = difference(self._downloading, downloaded)
        awaited = difference(self._downloading, left_to_download)
        self._available = union(self._available, awaited)
        self._downloading = left_to_download
