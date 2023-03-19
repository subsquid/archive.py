import time
from dataclasses import dataclass
from typing import Optional


@dataclass
class _Slot:
    time: float
    counter: int


class Rate:
    _window: list[_Slot]

    def __init__(self, window_size: int, slot_secs: float):
        self.window_size = window_size
        self.slot_secs = slot_secs
        self._window = []

    def inc(self, count: int = 1, current_time: Optional[float] = None) -> None:
        current_time = current_time or time.time()
        if not self._window or current_time > self._window[-1].time + self.slot_secs:
            self._window.append(_Slot(current_time, 1))
        else:
            self._window[-1].counter += count
        self._remove_old(current_time)

    def get(self, current_time: Optional[float] = None) -> int:
        current_time = current_time or time.time()
        cutoff = self._cutoff_time(current_time)
        return sum(s.counter for s in self._window if s.time >= cutoff)

    def _remove_old(self, current_time: float):
        cutoff = self._cutoff_time(current_time)
        while self._window and self._window[0].time < cutoff:
            self._window.pop(0)

    def _cutoff_time(self, current_time: float):
        return current_time - (self.window_size + 1) * self.slot_secs
