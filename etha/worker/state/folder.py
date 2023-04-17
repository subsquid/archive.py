import logging
from typing import Callable

from etha.fs import create_fs, LocalFs
from etha.layout import DataChunk, get_chunks
from etha.worker.state.controller import State, StateUpdate
from etha.worker.state.dataset import dataset_decode, dataset_encode
from etha.worker.state.intervals import to_range_set


class StateFolder:
    def __init__(self, data_dir: str):
        self.fs = LocalFs(data_dir)

    def read_state(self) -> State:
        state: State = {}
        for item in self.fs.ls():
            try:
                ds = dataset_decode(item)
            except ValueError:
                continue
            state[ds] = to_range_set(get_chunks(self.fs.cd(item)))
        return state

    def apply_update(self,
                     update: StateUpdate,
                     on_downloaded_chunk: Callable[[str, DataChunk], None] = lambda ds, chunk: None,
                     log=logging.getLogger(__name__)
                     ):

        log.info('update task started')

        # delete old chunks and datasets
        for ds, upd in update.items():
            fs = self.fs.cd(dataset_encode(ds))
            if upd:
                for deleted in upd.deleted:
                    for chunk in get_chunks(fs, first_block=deleted[0], last_block=deleted[1]):
                        log.info(f'deleting chunk {ds}/{chunk.path()} at {fs.abs()}')
                        fs.delete(chunk.path())
            else:
                log.info(f'deleting dataset {ds} at {fs.abs()}')
                fs.delete('.')

        # download new chunks
        for ds, upd in update.items():
            fs = self.fs.cd(dataset_encode(ds))
            rfs = create_fs(ds)
            for new in upd.new:
                for chunk in get_chunks(rfs, first_block=new[0], last_block=new[1]):
                    dest = fs.abs(chunk.path())
                    with fs.transact(dest) as d:
                        src = chunk.path()
                        log.info(f'downloading {rfs.abs(src)}')
                        # FIXME: we can potentially miss files here
                        rfs.download(src, d.abs())
                    log.info('saved %s', dest)
                    on_downloaded_chunk(ds, chunk)

        log.info('update task finished')
