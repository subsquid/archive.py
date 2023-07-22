import asyncio
import multiprocessing as mp
from itertools import groupby
from queue import Empty
from typing import Callable

from etha.worker.state.controller import State, StateUpdate
from etha.worker.state.folder import StateFolder
from etha.worker.state.intervals import to_range_set
from etha.worker.state.sync_proc import sync_proc


class _SyncProc:
    def __init__(self, data_dir: str):
        self.update_queue = mp.Queue(100)
        self.new_chunks_queue = mp.Queue(1000)
        self.proc = mp.Process(
            target=sync_proc,
            args=(data_dir, self.update_queue, self.new_chunks_queue),
            name='sm:data_sync'
        )

    def close(self):
        if self.proc.is_alive():
            self.proc.kill()
            self.proc.join()
        self.proc.close()
        self.update_queue.close()
        self.new_chunks_queue.close()

    def poll(self) -> State:
        if self.proc.is_alive():
            ds_chunk_pairs = []
            while True:
                try:
                    pair = self.new_chunks_queue.get_nowait()
                    ds_chunk_pairs.append(pair)
                except Empty:
                    break
            ds_chunk_pairs.sort()
            return {
                ds: to_range_set((p[1][0], p[1][1]) for p in pairs) for ds, pairs in groupby(
                    ds_chunk_pairs, key=lambda p: p[0]
                )
            }
        elif self.proc.exitcode is None:
            self.proc.start()
            return {}
        else:
            raise Exception(f'Data sync process unexpectedly terminated with exit code {self.proc.exitcode}')

    def start(self):
        self.proc.start()


class SyncProcess:
    def __init__(self, data_dir: str):
        self._data_dir = data_dir
        self._download_callback = None
        self._proc = _SyncProc(data_dir)

    def set_download_callback(self, cb: Callable[[State], None]) -> None:
        self._download_callback = cb

    def send_update(self, upd: StateUpdate) -> None:
        self._proc.update_queue.put_nowait(upd)

    def reset(self) -> State:
        self._proc.close()
        self._proc = _SyncProc(self._data_dir)
        self._proc.start()
        return StateFolder(self._data_dir).read_state()

    async def run(self):
        while True:
            downloaded = self._proc.poll()

            if self._download_callback and downloaded:
                self._download_callback(downloaded)

            await asyncio.sleep(5)

    def close(self):
        self._proc.close()
