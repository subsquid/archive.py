from dataclasses import dataclass
from time import time as now


@dataclass
class ProgressUnit:
    value: int
    time: float


class Progress:
    def __init__(self, window_size: int = 50, window_granularity_seconds: int = 0):
        assert window_size > 1
        assert window_granularity_seconds >= 0
        self._window: list[ProgressUnit] = []
        self._tail = 0
        self._size = window_size + 1
        self._granularity = window_granularity_seconds
        self._has_news = False

    def set_current_value(self, value: int, time: float | None = None):
        if time is None:
            time = now()

        if len(self._window) == 0:
            self._window.append(ProgressUnit(value, time))
            self._tail = 1
            return

        last = self._last()
        value = max(value, last.value)
        if time <= last.time:
            last.value = value
        elif len(self._window) > 1 and time <= last.time + self._granularity:
            last.value = value
        else:
            unit = ProgressUnit(value, time)
            if self._tail < len(self._window):
                self._window[self._tail] = unit
            else:
                self._window.append(unit)
            self._tail = (self._tail + 1) % self._size

        self._has_news = True

    def get_current_value(self) -> int:
        assert len(self._window) > 0, 'no current value available'
        return self._last().value

    def has_news(self) -> bool:
        return self._has_news

    def speed(self) -> float:
        self._has_news = False
        if len(self._window) < 2:
            return 0
        beg = self._window[0] if len(self._window) < self._size else self._window[self._tail]
        end = self._last()
        duration = end.time - beg.time
        inc = end.value - beg.value
        return inc / duration

    def _last(self) -> ProgressUnit:
        assert len(self._window) > 0
        return self._window[(self._size + self._tail - 1) % self._size]
