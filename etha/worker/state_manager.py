import datetime
from typing import NamedTuple

from .intervals import difference, Range, union


class DataStateUpdate(NamedTuple):
    new_ranges: list[Range]
    deleted_ranges: list[Range]


class DataState:
    def __init__(self, available_ranges: list[Range], delete_timeout_secs=30):
        self._available = available_ranges
        self._downloading = []
        self._to_delete = []
        self._delete_timeout = datetime.timedelta(seconds=delete_timeout_secs)

    def get_available_ranges(self) -> list[Range]:
        return self._available

    def _get_present(self):
        sets = [rs for ts, rs in self._to_delete]
        sets.append(self._available)
        return list(union(*sets))

    def ping(self, desired_ranges: list[Range]) -> DataStateUpdate:
        present_ranges = self._get_present()

        new_ranges = list(difference(desired_ranges, union(present_ranges, self._downloading)))

        available_to_delete = list(difference(self._available, desired_ranges))
        downloading_to_delete = list(difference(self._downloading, desired_ranges))

        self._to_delete.append((datetime.datetime.now(), available_to_delete))

        self._downloading = list(difference(desired_ranges, present_ranges))

        self._available = list(difference(desired_ranges, self._downloading))

        deleted_ranges = self._update_to_delete(desired_ranges)
        deleted_ranges = list(union(deleted_ranges, downloading_to_delete))

        return DataStateUpdate(new_ranges=new_ranges, deleted_ranges=deleted_ranges)

    def _update_to_delete(self, desired_ranges: list[Range]):
        now = datetime.datetime.now()
        sets = []
        to_delete = []

        for ts, rs in self._to_delete:
            rs = list(difference(rs, desired_ranges))
            if rs:
                if now - ts > self._delete_timeout:
                    sets.append(rs)
                else:
                    to_delete.append((ts, rs))

        self._to_delete = to_delete
        return list(union(*sets))

    def add_downloaded_range(self, r: Range):
        waiting = difference([r], difference([r], self._downloading))
        self._available = list(union(self._available, waiting))
        self._downloading = list(difference(self._downloading, [r]))
