import argparse
import asyncio
import logging
import multiprocessing
import os

import httpx
import uvicorn

from sqa.util.asyncio import create_child_task, monitor_service_tasks, run_async_program
from sqa.util.child_proc import init_child_process
from sqa.worker.api import create_app
from sqa.worker.state.controller import State
from sqa.worker.state.intervals import to_range_set
from sqa.worker.state.manager import StateManager
from sqa.worker.worker import Worker


LOG = logging.getLogger(__name__)


class HttpTransport:
    def __init__(self,  worker_id: str, worker_url: str, router_url: str):
        self._worker_id = worker_id
        self._worker_url = worker_url
        self._router_url = router_url
        self._state_updates = asyncio.Queue(maxsize=100)

    async def send_ping(self, state: State, stored_bytes: int, pause=False):
        ping_msg = {
            'worker_id': self._worker_id,
            'worker_url': self._worker_url + '/query',
            'state': state,
            'pause': pause,
        }

        async with httpx.AsyncClient(base_url=self._router_url) as client:
            try:
                response = await client.post('/ping', json=ping_msg)
                response.raise_for_status()
                result = response.json()
                desired_state = {
                    ds: to_range_set(map(tuple, ranges)) for ds, ranges in result.items()
                }
                await self._state_updates.put(desired_state)
            except httpx.HTTPError:
                LOG.exception('failed to send a ping message')

    async def state_updates(self):
        while True:
            yield await self._state_updates.get()


class Server(uvicorn.Server):
    def install_signal_handlers(self) -> None:
        pass

    async def run_server_task(self) -> None:
        task = create_child_task('serve', self.serve())
        try:
            # wrap with `asyncio.wait()` to prevent immediate coroutine abortion
            await asyncio.wait([task])
        except asyncio.CancelledError:
            self.should_exit = True
            await task


def parse_cli_args():
    program = argparse.ArgumentParser(
        description='Subsquid eth archive worker'
    )

    program.add_argument(
        '--router',
        required=True,
        metavar='URL',
        help='URL of the router to connect to'
    )

    program.add_argument(
        '--worker-id',
        required=True,
        metavar='UID',
        help='unique id of this worker'
    )

    program.add_argument(
        '--worker-url',
        required=True,
        metavar='URL',
        help='externally visible URL of this worker'
    )

    program.add_argument(
        '--data-dir',
        metavar='DIR',
        help='directory to keep in the data and state of this worker (defaults to cwd)'
    )

    program.add_argument(
        '--port',
        type=int,
        default=8000,
        metavar='N',
        help='port to listen on (defaults to 8000)'
    )

    program.add_argument(
        '--procs',
        type=int,
        metavar='N',
        help='number of processes to use to execute data queries'
    )

    return program.parse_args()


async def serve(args):
    sm = StateManager(data_dir=args.data_dir or os.getcwd())

    transport = HttpTransport(
        worker_id=args.worker_id,
        worker_url=args.worker_url,
        router_url=args.router,
    )

    with multiprocessing.Pool(processes=args.procs, initializer=init_child_process) as pool:
        worker = Worker(sm, pool, transport)
        app = create_app(sm, worker)
        server_conf = uvicorn.Config(
            app,
            port=args.port,
            host='0.0.0.0',
            access_log=False,
            log_config=None
        )

        server = Server(server_conf)

        await monitor_service_tasks([
            asyncio.create_task(server.run_server_task(), name='http_server'),
            asyncio.create_task(sm.run(), name='state_manager'),
            asyncio.create_task(worker.run(), name='worker'),
        ])


def cli():
    if os.getenv('SENTRY_DSN'):
        import sentry_sdk
        sentry_sdk.init(
            traces_sample_rate=1.0
        )
    run_async_program(serve, parse_cli_args())
