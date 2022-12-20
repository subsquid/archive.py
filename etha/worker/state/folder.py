import logging
import os
from typing import Callable, Optional

from etha.fs import create_fs, LocalFs
from etha.layout import DataChunk, get_chunks
from etha.worker.state.controller import State, StateUpdate
from etha.worker.state.intervals import to_range_set


class StateFolder:
    def __init__(self, data_dir: str):
        self.fs = LocalFs(data_dir)

    def read_dataset(self) -> Optional[str]:
        cfg_file = self.fs.abs('dataset.txt')
        try:
            with open(cfg_file, encoding='utf-8') as f:
                return f.read().strip()
        except FileNotFoundError:
            return None

    def save_dataset(self, dataset: str):
        cfg_file = self.fs.abs('dataset.txt')
        os.makedirs(os.path.dirname(cfg_file), exist_ok=True)
        with open(cfg_file, 'w', encoding='utf-8') as f:
            f.write(dataset)

    def read_state(self) -> State:
        ds = self.read_dataset()
        if ds:
            ranges = to_range_set((c[0], c[1]) for c in get_chunks(self.fs.cd('dataset')))
            return State(ds, ranges)
        else:
            return State('', [])

    def apply_update(self,
                     update: StateUpdate,
                     on_downloaded_chunk: Callable[[str, DataChunk], None] = lambda chunk: None,
                     log=logging.getLogger(__name__)
                     ):

        def rm(item: str):
            log.info(f'deleting {self.fs.abs(item)}')
            self.fs.delete(item)

        log.info('update task started')

        if self.read_dataset() != update.dataset:
            log.info('switching to a new dataset %s', update.dataset)
            rm('dataset.txt')
            rm('dataset')
            self.save_dataset(update.dataset)

        # delete old chunks
        for deleted_range in update.deleted_ranges:
            for chunk in get_chunks(self.fs.cd('dataset'), first_block=deleted_range[0], last_block=deleted_range[1]):
                rm(f'dataset/{chunk.path()}')

        # download new chunks
        rfs = create_fs(update.dataset)
        for new_range in update.new_ranges:
            for chunk in get_chunks(rfs, first_block=new_range[0], last_block=new_range[1]):
                dest = self.fs.abs(f'dataset/{chunk.path()}')
                with self.fs.transact(dest) as d:
                    for f in ['transactions', 'logs', 'blocks']:
                        src = f'{chunk.path()}/{f}.parquet'
                        log.info(f'downloading {rfs.abs(src)}')
                        rfs.download(src, d.abs(f'{f}.parquet'))
                log.info('saved %s', dest)
                on_downloaded_chunk(update.dataset, chunk)

        log.info('update task finished')
