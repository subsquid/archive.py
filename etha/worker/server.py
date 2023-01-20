import argparse
import asyncio
import logging
import multiprocessing
import os
import signal
import threading
import time

import uvicorn

from etha.log import init_logging
from etha.worker.api import create_app
from etha.worker.state.manager import StateManager

LOG = logging.getLogger(__name__)


def main():
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

    def initializer():
        init_logging()
        signal.signal(signal.SIGINT, signal.SIG_IGN)

    with multiprocessing.Pool(processes=args.procs, initializer=initializer) as pool:
        app = create_app(sm, pool)
        conf = uvicorn.Config(
            app,
            port=args.port,
            host='0.0.0.0',
            access_log=False,
            log_config=None
        )
        server = uvicorn.Server(conf)

        async def run():
            server_task = asyncio.create_task(server.serve(), name='server')
            sm_task = asyncio.create_task(sm.run(), name='state_manager')

            await asyncio.wait([
                server_task,
                sm_task
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

        # It is spawned in a new thread so that uvicorn doesn't register signal handler,
        # and we can control application shutdown.
        t = threading.Thread(target=asyncio.run, args=(run(),))
        t.start()

        while True:
            try:
                time.sleep(1000000)
            except KeyboardInterrupt:
                server.handle_exit(signal.SIGINT, frame=None)
                break

        t.join(timeout=5)


if __name__ == '__main__':
    init_logging()
    main()
