import asyncio
import logging
import signal
import sys

from etha.log import init_logging


LOG = logging.getLogger(__name__)


def add_temp_prefix(path: str) -> str:
    import datetime
    import os.path
    now = datetime.datetime.now()
    ts = round(now.timestamp() * 1000)
    name = os.path.basename(path)
    parent = os.path.dirname(path)
    return os.path.join(parent, f'temp-{ts}-{name}')


def init_child_process() -> None:
    init_logging()
    signal.signal(signal.SIGINT, signal.SIG_IGN)


def create_child_task(name: str, coro) -> asyncio.Task:
    parent_name = asyncio.current_task().get_name()
    task_name = name if parent_name.startswith('Task-') else f'{parent_name}:{name}'
    return asyncio.create_task(coro, name=task_name)


async def monitor_service_tasks(tasks: list[asyncio.Task], log=LOG) -> None:
    try:
        await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
    except asyncio.CancelledError:
        pass

    exception = None

    for task in reversed(tasks):
        if task.done():
            if exception:
                log.error(f'task {task.get_name()} unexpectedly terminated', exc_info=task.exception())
            else:
                exception = task.exception() or Exception(f'task {task.get_name()} unexpectedly terminated')

    for task in reversed(tasks):
        if not task.done():
            task.cancel()
            try:
                log.debug('waiting for %s to complete', task.get_name())
                await task
            except asyncio.CancelledError:
                pass
            except Exception as ex:
                if exception:
                    log.error(f'failed to gracefully terminate task {task.get_name()}', exc_info=ex)
                else:
                    exception = ex

    if exception:
        raise exception


def wait_for_term_signal():
    loop = asyncio.get_event_loop()
    future = loop.create_future()

    def handler(signal_name: str):
        future.set_result(signal_name)

    loop.add_signal_handler(signal.SIGINT, handler, 'SIGINT')
    loop.add_signal_handler(signal.SIGTERM, handler, 'SIGTERM')

    return future


def run_async_program(main_coro, log=LOG):
    init_logging()

    async def run():
        signal_future = wait_for_term_signal()
        program = asyncio.create_task(main_coro)
        await asyncio.wait([signal_future, program], return_when=asyncio.FIRST_COMPLETED)

        if signal_future.done() and not program.done():
            log.error(f'terminating program due to {signal_future.result()}')
            program.cancel()

        await program

    try:
        asyncio.run(run())
    except asyncio.CancelledError:
        sys.exit(1)
    except:
        log.exception('program crashed')
        sys.exit(1)
