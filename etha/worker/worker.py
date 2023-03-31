import asyncio
import logging
from multiprocessing.pool import Pool

from etha.query.model import Query
from etha.util import create_child_task, monitor_service_tasks
from etha.worker.query import QueryResult, execute_query
from etha.worker.state.dataset import dataset_decode
from etha.worker.state.manager import StateManager
from etha.worker.transport import Transport

LOG = logging.getLogger(__name__)
PING_INTERVAL_SEC = 10


class QueryError(Exception):
    pass


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
            LOG.info('state update', extra=desired_state)
            self._sm.update_state(desired_state)

    async def _ping_loop(self):
        try:
            while not self._shutdown:
                state = self._sm.get_state()
                await self._transport.send_ping(state)
                await asyncio.sleep(PING_INTERVAL_SEC)
        finally:
            await self._pause_ping()

    async def _pause_ping(self):
        state = self._sm.get_state()
        try:
            async with asyncio.timeout(1):
                await self._transport.send_ping(state, pause=True)
        except:
            LOG.exception('failed to send a pause ping')

    async def execute_query(self, query: Query, dataset: str) -> QueryResult:
        try:
            dataset = dataset_decode(dataset)
        except ValueError:
            raise QueryError(f'failed to decode dataset: {dataset}')

        first_block = query['fromBlock']
        last_block = query.get('toBlock')
        if last_block is not None and last_block < first_block:
            raise QueryError(f'fromBlock={last_block} > toBlock={first_block}')

        data_range_lock = self._sm.use_range(dataset, first_block)
        if data_range_lock is None:
            raise QueryError(f'data for block {first_block} is not available')

        with data_range_lock as data_range:
            args = self._sm.get_temp_dir(), self._sm.get_dataset_dir(dataset), data_range, query
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