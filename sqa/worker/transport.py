from typing import AsyncIterator, Protocol

from .state.controller import State


class Transport(Protocol):
    async def send_ping(self, state: State, stored_bytes: int, pause=False) -> None:
        pass

    def state_updates(self) -> AsyncIterator[State]:
        pass
