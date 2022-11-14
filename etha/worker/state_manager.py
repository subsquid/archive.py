import asyncio
import logging
import multiprocessing as mp
import os.path
from queue import Empty
from time import sleep
from typing import Callable, NamedTuple, Optional

import httpx

from .intervals import difference, Range, remove_intersections, union
from ..fs import create_fs, LocalFs
from ..layout import DataChunk, get_chunks

LOG = logging.getLogger(__name__)


class DataStateUpdate(NamedTuple):
    new_ranges: list[Range]
    deleted_ranges: list[Range]


class DataState:
    def __init__(self, available_ranges: list[Range]):
        self._available = available_ranges
        self._downloading = []

    def get_available_ranges(self) -> list[Range]:
        return self._available

    def get_downloading_ranges(self) -> list[Range]:
        return self._downloading

    def get_range(self, first_block: int) -> Optional[Range]:
        pass

    def ping(self, desired_ranges: list[Range]) -> DataStateUpdate:
        new_ranges = list(difference(desired_ranges, union(self._available, self._downloading)))

        deleted_ranges = list(difference(union(self._available, self._downloading), desired_ranges))

        self._downloading = list(difference(desired_ranges, self._available))

        self._available = list(difference(desired_ranges, self._downloading))

        return DataStateUpdate(new_ranges=new_ranges, deleted_ranges=deleted_ranges)

    def add_downloaded_ranges(self, ranges: list[Range]):
        waiting = difference(ranges, difference(ranges, self._downloading))
        self._available = list(union(self._available, waiting))
        self._downloading = list(difference(self._downloading, ranges))


def _get_dataset(data_dir: str) -> Optional[str]:
    file = os.path.join(data_dir, 'dataset.txt')
    if os.path.exists(file):
        with open(file, encoding='utf-8') as f:
            return f.read().strip()


def _save_dataset(data_dir: str, dataset: str):
    cfg_file = os.path.join(data_dir, 'dataset.txt')
    with open(cfg_file, 'w', encoding='utf-8') as f:
        f.write(dataset)


def _update_state(
        data_dir: str,
        dataset: str,
        update: DataStateUpdate,
        on_downloaded_chunk: Callable[[DataChunk], None] = lambda chunk: None
):
    lfs = LocalFs(data_dir)

    pause_deletion = True

    def rm(item: str):
        nonlocal pause_deletion
        if pause_deletion:
            # pause in case some pending request still uses the data
            # FIXME: this measure merely reduces the probability of data inconsistency, but doesn't guarantee it
            sleep(10)
            pause_deletion = False
        LOG.info(f'deleting {lfs.abs(item)}')
        lfs.delete(item)

    if _get_dataset(data_dir) != dataset:
        # delete old dataset
        LOG.info('switching to a new dataset %s', dataset)
        rm('dataset.txt')
        rm('dataset')
        _save_dataset(data_dir, dataset)

    # delete old chunks
    for deleted_range in update.deleted_ranges:
        for chunk in get_chunks(lfs.cd('dataset'), first_block=deleted_range[0], last_block=deleted_range[1]):
            rm(f'dataset/{chunk.path()}')

    # download new chunks
    dfs = create_fs(dataset)
    for new_range in update.new_ranges:
        for chunk in get_chunks(dfs, first_block=new_range[0], last_block=new_range[1]):
            dest = lfs.abs(f'dataset/{chunk.path()}')
            temp = lfs.abs(f'dataset/temp-{chunk.path()}')
            LOG.info(f'downloading {chunk.path()}')
            dfs.download(chunk.path(), temp)
            os.rename(dest, temp)
            LOG.info('saved %s', dest)
            on_downloaded_chunk(chunk)


def _run_update_loop(data_dir: str, updates_queue: mp.Queue, new_chunks_queue: mp.Queue):
    while True:
        dataset, upd = updates_queue.get()
        LOG.info('starting update task')
        _update_state(
            data_dir,
            dataset,
            upd,
            on_downloaded_chunk=lambda chunk: new_chunks_queue.put((dataset, chunk))
        )
        LOG.info('update task finished')


class _State(NamedTuple):
    dataset: str
    ranges: DataState


class StateManager:
    def __init__(self, data_dir: str, worker_id: str, worker_url: str, router_url: str):
        self._data_dir = data_dir
        self._worker_id = worker_id
        self._worker_url = worker_url
        self._router_url = router_url
        self._state = self._read_state()
        self._updates_queue = mp.Queue(10)
        self._new_chunks_queue = mp.Queue(100)
        self._is_started = False

    def _read_state(self) -> Optional[_State]:
        dataset = _get_dataset(self._data_dir)
        if not dataset:
            return None

        lfs = LocalFs(os.path.join(self._data_dir, 'dataset'))
        available_ranges = list(remove_intersections(get_chunks(lfs)))
        state = DataState(available_ranges)
        return _State(dataset, state)

    @property
    def data_dir(self):
        return self._data_dir

    def get_dataset(self) -> Optional[str]:
        if self._state:
            return self._state.dataset

    def get_range(self, first_block) -> Optional[Range]:
        if self._state:
            return self._state.ranges.get_range(first_block)

    def get_ping_message(self):
        msg = {
            'worker_id': self._worker_id,
            'worker_url': self._worker_url
        }
        if self._state:
            msg['state'] = {
                'dataset': self._state.dataset,
                'ranges': [{'from': r[0], 'to': r[1]} for r in self._state.ranges.get_available_ranges()]
            }
        return msg

    def get_status(self):
        state = self._state
        return {
            'worker_id': self._worker_id,
            'worker_url': self._worker_url,
            'dataset': state.dataset if state else 'none',
            'available_ranges': state.ranges.get_available_ranges() if state else [],
            'downloading_ranges': state.ranges.get_downloading_ranges() if state else []
        }

    async def _update_loop(self):
        p = mp.Process(
            target=_run_update_loop,
            args=(self._data_dir, self._updates_queue, self._new_chunks_queue),
            name='sm:data_update'
        )
        p.start()
        try:
            while True:
                await asyncio.sleep(10)
                if not p.is_alive():
                    p.join()
                    raise Exception(f'data update process terminated with exitcode {p.exitcode}')
        except asyncio.CancelledError as ex:
            p.terminate()
            p.join()
            raise ex
        finally:
            p.close()

    async def _ping_loop(self):
        async with httpx.AsyncClient() as client:
            while True:
                await self._ping(client)
                await asyncio.sleep(10)

    async def _ping(self, client: httpx.AsyncClient):
        try:
            response = await client.post(self._router_url, json=self.get_ping_message())
            response.raise_for_status()
        except httpx.HTTPError:
            LOG.exception('failed to send a ping message')
            return

        ping = response.json()
        desired_dataset = ping['dataset']
        desired_ranges = [(r['from'], r['to']) for r in ping['ranges']]

        if self._state is None or self._state.dataset != desired_dataset:
            self._state = _State(desired_dataset, DataState([]))

        upd = self._state.ranges.ping(desired_ranges)

        self._updates_queue.put((desired_dataset, upd))

    async def _chunk_receive_loop(self):
        while True:
            await asyncio.sleep(1)

            ranges = []
            while True:
                try:
                    dataset, chunk = self._new_chunks_queue.get_nowait()
                    if self._state and self._state.dataset == dataset:
                        ranges.append((chunk.first_block, chunk.last_block))
                except Empty:
                    break

            if ranges:
                ranges.sort()
                ranges = list(remove_intersections(ranges))
                self._state.ranges.add_downloaded_ranges(ranges)

    async def run(self):
        assert not self._is_started
        self._is_started = True
        update_loop = asyncio.create_task(self._update_loop(), name='sm:data_update_loop')
        ping_loop = asyncio.create_task(self._ping_loop(), name='sm:ping_loop')
        chunk_recv_loop = asyncio.create_task(self._chunk_receive_loop(), name='sm:chunk_receive_loop')
        await _monitor_service_tasks(update_loop, ping_loop, chunk_recv_loop)


async def _monitor_service_tasks(*tasks: asyncio.Task):
    # FIXME: validate this logic
    try:
        await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
    except asyncio.CancelledError as ex:
        for task in tasks:
            task.cancel()
        raise ex

    terminated = []

    for task in tasks:
        if task.done():
            terminated.append(task)
        else:
            task.cancel()

    tt = terminated[0]
    raise Exception(f'Service task {tt.get_name()} unexpectedly terminated') from tt.exception()
