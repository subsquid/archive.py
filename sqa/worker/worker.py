import asyncio
import logging
from multiprocessing.pool import Pool

from sqa.query.builder import ArchiveQuery
from sqa.util.asyncio import create_child_task, monitor_service_tasks
from .query import execute_query, QueryResult, QueryError, validate_query
from .state.dataset import dataset_decode
from .state.manager import StateManager
from .transport import Transport


LOG = logging.getLogger(__name__)
PING_INTERVAL_SEC = 10


class Worker:
    def __init__(self, sm: StateManager, pool: Pool, transport: Transport):
        self._sm = sm
        self._pool = pool
        self._transport = transport
        self._shutdown = False

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
        try:
            dataset = dataset_decode(dataset)
        except ValueError:
            raise QueryError(f'failed to decode dataset: {dataset}')

        query = validate_query(query)

        first_block = query['fromBlock']

        data_range_lock = self._sm.use_range(dataset, first_block)
        if data_range_lock is None:
            raise QueryError(f'data for block {first_block} is not available')

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
