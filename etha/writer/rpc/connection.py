from typing import Optional

from etha.writer.speed import Speed


class Connection:
    def __init__(self, url: str, limit: Optional[int]):
        self.url = url
        self.limit = limit
        self._speed = Speed(window_size=200)

    def average_response_time(self):
        return self._speed.time() or 0.01

    def __hash__(self):
        return hash(self.url)
