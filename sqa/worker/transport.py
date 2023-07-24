import abc
from typing import AsyncIterator

from sqa.worker.state.controller import State


class Transport(abc.ABC):

    @abc.abstractmethod
    async def send_ping(self, state: State, stored_bytes: int, pause=False) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    def state_updates(self) -> AsyncIterator[State]:
        raise NotImplementedError
