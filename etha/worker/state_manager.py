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

        new_ranges = list(difference(desired_ranges, present_ranges))

        new_to_delete = list(difference(union(self._available, self._downloading), desired_ranges))

        self._to_delete.append((datetime.datetime.now(), new_to_delete))

        self._downloading = list(difference(desired_ranges, self._available))

        self._available = list(difference(present_ranges, new_to_delete))

        deleted_ranges = self._check_deleted_ranges(desired_ranges)

        return DataStateUpdate(new_ranges=new_ranges, deleted_ranges=deleted_ranges)

    def _check_deleted_ranges(self, desired_ranges: list[Range]):
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
