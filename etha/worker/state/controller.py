import bisect
import contextlib
from dataclasses import dataclass, field
from typing import NamedTuple, Optional, Protocol, Union, Literal, Callable

from etha.worker.state.dataset import Dataset
from etha.worker.state.intervals import difference, Range, RangeSet, to_range_set, union


RangeLock = contextlib.AbstractContextManager[Range]


State = dict[Dataset, RangeSet]


class UpdatedRanges(NamedTuple):
    new: RangeSet
    deleted: RangeSet


StateUpdate = dict[Dataset, Union[Literal[False], UpdatedRanges]]


class Sync(Protocol):
    def send_update(self, upd: StateUpdate) -> None:
        pass

    def reset(self) -> State:
        pass

    def set_download_callback(self, cb: Callable[[State], None]) -> None:
        pass


@dataclass
class _DatasetTrack:
    available: RangeSet = field(default_factory=list)
    downloading: RangeSet = field(default_factory=list)
    to_delete: RangeSet = field(default_factory=list)
    locked: dict[Range, int] = field(default_factory=dict)
    is_live: bool = True

    def get_range(self, first_block: int) -> Optional[Range]:
        if not self.available:
            return

        i = bisect.bisect_left(self.available, (first_block, first_block))

        if i < len(self.available):
            c = self.available[i]
            if c[0] == first_block:
                return c

        if i == 0:
            return

        c = self.available[i-1]
        if c[1] >= first_block:
            return c

    @contextlib.contextmanager
    def lock_range(self, r: Range) -> RangeLock:
        self.locked[r] = self.locked.get(r, 0) + 1
        try:
            yield r
        finally:
            cnt = self.locked[r]
            if cnt > 1:
                self.locked[r] = cnt - 1
            else:
                del self.locked[r]

    def use_range(self, first_block: int) -> Optional[RangeLock]:
        r = self.is_live and self.get_range(first_block)
        if r:
            return self.lock_range(r)

    def is_used(self) -> bool:
        return bool(self.locked)

    def ping(self, desired_ranges: RangeSet) -> Optional[UpdatedRanges]:
        assert not difference(self.downloading, desired_ranges)

        self.is_live = True

        available = union(self.available, self.to_delete)

        new_ranges = difference(desired_ranges, union(available, self.downloading))
        deleted_ranges = difference(available, desired_ranges)

        locked = to_range_set(self.locked.keys())
        to_delete_now = difference(deleted_ranges, locked)

        self.to_delete = difference(deleted_ranges, to_delete_now)
        self.downloading = difference(desired_ranges, available)
        self.available = difference(desired_ranges, self.downloading)

        if new_ranges or to_delete_now:
            return UpdatedRanges(new=new_ranges, deleted=to_delete_now)

    def download_callback(self, downloaded: RangeSet) -> None:
        assert not difference(downloaded, self.downloading)
        self.available = union(self.available, downloaded)
        self.downloading = difference(self.downloading, downloaded)


class DatasetStatus(NamedTuple):
    available_ranges: RangeSet
    downloading_ranges: RangeSet


class StateController:
    _track: dict[Dataset, _DatasetTrack]

    def __init__(self, sync: Sync):
        self._sync = sync
        self._sync.set_download_callback(self._download_callback)
        self._track = {}
        self._reset()

    def get_status(self) -> dict[Dataset, DatasetStatus]:
        status = {}
        for dataset, track in self._track.items():
            if track.is_live:
                status[dataset] = DatasetStatus(
                    available_ranges=track.available,
                    downloading_ranges=track.downloading
                )
        return status

    def get_state(self) -> State:
        return {
            ds: track.available for ds, track in self._track.items() if track.is_live and track.available
        }

    def use_range(self, dataset: str, first_block: int) -> Optional[RangeLock]:
        track = self._track.get(dataset)
        if track:
            return track.use_range(first_block)

    def ping(self, desired_state: State) -> None:
        upd: StateUpdate = {}

        for dataset, desired_ranges in desired_state.items():
            track = self._track.get(dataset)
            if not track:
                track = self._track[dataset] = _DatasetTrack()

            if difference(track.downloading, desired_ranges):
                # Some data ranges scheduled for downloading are not needed anymore.
                # This may be a sign of significant lag between the update process and the scheduler.
                # To eliminate the lag we restart the update process and check the current state
                # to issue a single update leading to the desired state.
                #
                # By this measure we also prevent scenarios like:
                #   1. downloading chunk A in update task 1
                #   2. chunk A becomes no longer desired, schedule task 2 to delete A
                #   3. chunk A becomes desired again, schedule task 3 to download A
                #   4. task 1 completes and notifies controller about A's availability
                #   5. controller can mark A as available, but A is about to be deleted by task 2
                self._reset()
                return self.ping(desired_state)

            if updated_ranges := track.ping(desired_ranges):
                upd[dataset] = updated_ranges

        for dataset, track in self._track.items():
            if dataset not in desired_state:
                if track.downloading:
                    self._reset()
                    return self.ping(desired_state)

                track.is_live = False
                if not track.is_used():
                    del self._track[dataset]
                    upd[dataset] = False

        if upd:
            self._sync.send_update(upd)

    def _reset(self):
        state = self._sync.reset()
        old_track = self._track
        self._track = {}
        for dataset, available_ranges in state.items():
            self._track[dataset] = _DatasetTrack(
                available=available_ranges,
                locked=old_track.get(dataset, _DatasetTrack()).locked
            )

    def _download_callback(self, new_ranges: State) -> None:
        for dataset, downloaded in new_ranges.items():
            self._track[dataset].download_callback(downloaded)
