import abc
from typing import AsyncIterator

from etha.worker.state.controller import State


class Transport(abc.ABC):

    @abc.abstractmethod
    async def send_ping(self, state: State, pause=False) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    def state_updates(self) -> AsyncIterator[State]:
        raise NotImplementedError
