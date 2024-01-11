import asyncio
import logging
import multiprocessing
import os

from sqa.query import MissingData
from sqa.query.schema import ArchiveQuery
from sqa.util.asyncio import create_child_task, monitor_service_tasks
from sqa.util.child_proc import init_child_process
from .query import QueryResult, validate_query, execute_query
from .state.manager import StateManager
from .transport import Transport


LOG = logging.getLogger(__name__)
PING_INTERVAL_SEC = int(os.environ.get('PING_INTERVAL_SEC', '10'))


class Worker:
    def __init__(self, sm: StateManager, transport: Transport, procs: int | None = None):
        self._sm = sm
        self._transport = transport
        self._procs = procs or (os.cpu_count() or 1) * 3 // 2
        self._pool = multiprocessing.Pool(
            processes=self._procs,
            initializer=init_child_process,
            maxtasksperchild=10
        )
        self._shutdown = False

    def get_processes_count(self) -> int:
        return self._procs

    async def run(self):
        state_update_task = create_child_task('state_update', self._state_update_loop())
        ping_task = create_child_task('ping', self._ping_loop())
        await monitor_service_tasks([state_update_task, ping_task], log=LOG)

    async def _state_update_loop(self):
        state_updates = self._transport.state_updates()
        while not self._shutdown:
            desired_state = await anext(state_updates)
            LOG.info('state ping', extra={'desired_state': desired_state})
            self._sm.update_state(desired_state)

    async def _ping_loop(self):
        try:
            while not self._shutdown:
                state = self._sm.get_state()
                stored_bytes = await self._sm.get_stored_bytes()
                await self._transport.send_ping(state, stored_bytes)
                await asyncio.sleep(PING_INTERVAL_SEC)
        finally:
            pass
            # FIXME: for some reason httpx fails to work after SIGINT/SIGTERM
            # await self._pause_ping()

    async def _pause_ping(self):
        state = self._sm.get_state()
        stored_bytes = await self._sm.get_stored_bytes()
        try:
            async with asyncio.timeout(1):
                await self._transport.send_ping(state, stored_bytes, pause=True)
        except:
            LOG.exception('failed to send a pause ping')

    async def execute_query(self, query: ArchiveQuery, dataset: str, profiling: bool = False) -> QueryResult:
        query = validate_query(query)

        first_block = query['fromBlock']
        data_range_lock = self._sm.use_range(dataset, first_block)
        if data_range_lock is None:
            raise MissingData(f'data for block {first_block} is not available')

        with data_range_lock as data_range:
            args = self._sm.get_dataset_dir(dataset), data_range, query, profiling
            loop = asyncio.get_event_loop()
            future = loop.create_future()

            def on_done(res):
                loop.call_soon_threadsafe(future.set_result, res)

            def on_error(err):
                loop.call_soon_threadsafe(future.set_exception, err)

            self._pool.apply_async(
                execute_query,
                args=args,
                callback=on_done,
                error_callback=on_error
            )

            return await future
