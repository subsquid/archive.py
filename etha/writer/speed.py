from dataclasses import dataclass


@dataclass
class SpeedUnit:
    value: int
    time: float
    duration: float


class Speed:
    def __init__(self, window_size: int = 50, window_granularity_seconds: int = 0):
        assert window_size > 0
        assert window_granularity_seconds >= 0
        self._window = [SpeedUnit(0, 0.0, 0.0)]
        self._tail = 1
        self._value = 0
        self._duration = 0.0
        self._size = window_size + 1
        self._granularity = window_granularity_seconds

    def push(self, value: int, beg: float, end: float):
        self._duration += end - beg
        self._value += value
        last = self._window[(self._size + self._tail - 1) % self._size]
        if len(self._window) > 1 and last.time + self._granularity >= end:
            last.value = self._value
            last.duration = self._duration
        else:
            unit = SpeedUnit(self._value, end, self._duration)
            if self._tail < len(self._window):
                self._window[self._tail] = unit
            else:
                self._window.append(unit)
            self._tail = (self._tail + 1) % self._size

    def speed(self) -> float:
        if len(self._window) < 2:
            return 0
        beg = self._window[0] if len(self._window) < self._size else self._window[self._tail]
        end = self._window[(self._size + self._tail - 1) % self._size]
        duration = end.duration - beg.duration
        inc = end.value - beg.value
        return inc / duration

    def time(self) -> float:
        speed = self.speed()
        return 0 if speed == 0 else 1 / speed
