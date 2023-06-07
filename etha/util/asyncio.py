import asyncio
import logging
import signal
import sys
from typing import Iterable, Optional


LOG = logging.getLogger(__name__)


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

    await teardown(reversed(tasks), log)
    raise exception


async def teardown(tasks: Iterable[asyncio.Task], log=LOG) -> None:
    for task in tasks:
        if task.done():
            try:
                task.exception()
            except:
                pass
        else:
            task.cancel()
            try:
                log.debug('waiting for %s to complete', task.get_name())
                await task
            except asyncio.CancelledError:
                pass
            except Exception as ex:
                log.error(f'failed to gracefully terminate task {task.get_name()}', exc_info=ex)


async def monitor_pipeline(tasks: list[asyncio.Task], service_task: Optional[asyncio.Task] = None, log=LOG) -> None:
    assert tasks
    pending_tasks = tasks
    while pending_tasks:
        if service_task:
            ts = [*pending_tasks, service_task]
        else:
            ts = pending_tasks

        try:
            await asyncio.wait(ts, return_when=asyncio.FIRST_COMPLETED)
        except asyncio.CancelledError as ex:
            await teardown(ts, log)
            raise ex

        if service_task and service_task.done():
            await teardown(pending_tasks, log)
            exception = service_task.exception() or Exception(f'task {service_task.get_name()} unexpectedly terminated')
            raise exception

        new_pending_tasks = []

        for task in pending_tasks:
            if task.done():
                log.debug('task %s finished', task.get_name())
                exception = task.exception()
                if exception:
                    await teardown(ts, log)
                    raise exception
            else:
                new_pending_tasks.append(task)

        pending_tasks = new_pending_tasks

    if service_task:
        await teardown([service_task], log)


def wait_for_term_signal():
    loop = asyncio.get_event_loop()
    future = loop.create_future()

    def handler(signal_name: str):
        future.set_result(signal_name)

    loop.add_signal_handler(signal.SIGINT, handler, 'SIGINT')
    loop.add_signal_handler(signal.SIGTERM, handler, 'SIGTERM')

    return future


def run_async_program(main, *args, log=LOG):

    async def run():
        signal_future = wait_for_term_signal()
        program = asyncio.create_task(main(*args))
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
