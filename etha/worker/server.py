import argparse
import asyncio
import logging
import multiprocessing
import os

import uvicorn

from .api import create_app
from .state.manager import StateManager
from ..util import sigterm_future
from etha.log import init_logging

LOG = logging.getLogger(__name__)


async def main():
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
        help='port to listen on (defaults to 8000)'
    )

    program.add_argument(
        '--procs',
        type=int,
        help='number of processes to use to execute data queries'
    )

    args = program.parse_args()

    sm = StateManager(
        worker_id=args.worker_id,
        worker_url=args.worker_url,
        data_dir=args.data_dir or os.getcwd(),
        router_url=args.router
    )

    with multiprocessing.Pool(processes=args.procs, initializer=init_logging) as pool:
        app = create_app(sm, pool)
        conf = uvicorn.Config(
            app,
            port=args.port,
            host='0.0.0.0',
            access_log=False,
            log_config=None
        )
        server = uvicorn.Server(conf)

        server_task = asyncio.create_task(server.serve(), name='server')
        sm_task = asyncio.create_task(sm.run(), name='state_manager')

        await asyncio.wait([
            server_task,
            sm_task,
            sigterm_future()
        ], return_when=asyncio.FIRST_COMPLETED)

        ex = None

        if sm_task.done():
            ex = sm_task.exception()
        else:
            sm_task.cancel()

        if server_task.done():
            ex = ex or server_task.exception()
        else:
            server.should_exit = True
            try:
                await server_task
            except:
                LOG.exception('failed to gracefully terminate HTTP server')

        if ex:
            raise ex


if __name__ == '__main__':
    init_logging()
    asyncio.run(main())
