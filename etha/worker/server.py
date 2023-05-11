import argparse
import asyncio
import multiprocessing
import os

import uvicorn

from etha.util.asyncio import create_child_task, monitor_service_tasks, run_async_program
from etha.util.child_proc import init_child_process
from etha.worker.api import create_app
from etha.worker.state.manager import StateManager
from etha.worker.transport import HttpTransport
from etha.worker.worker import Worker

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
    run_async_program(serve, parse_cli_args())
