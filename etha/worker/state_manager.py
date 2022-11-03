import json
import os.path
from typing import Optional

from ..fs import LocalFs
from ..layout import DataChunk, get_chunks


class StateManager:
    dataset: Optional[str]
    datachunks: list[DataChunk]

    def __init__(self, state_dir: str, worker_id: str, worker_url: str):
        self.state_dir = state_dir
        self.worker_id = worker_id
        self.worker_url = worker_url
        self._init_state()

    def _init_state(self):
        self.dataset = None
        self.datachunks = []

        state_file = self.path('state.json')
        if not os.path.exists(state_file):
            return

        with open(state_file) as f:
            state = json.load(f)
            self.dataset = state.get('dataset')

        if self.dataset:
            self.datachunks = list(get_chunks(LocalFs(self.path('datasets', self.dataset))))

    def get_status(self):
        ranges = []

        for chunk in self.datachunks:
            if ranges and ranges[-1]['to'] + 1 == chunk.first_block:
                ranges[-1]['to'] = chunk.last_block
            else:
                ranges.append({'from': chunk.first_block, 'to': chunk.last_block})

        return {
            'worker_id': self.worker_id,
            'worker_url': self.worker_url,
            'state': {
                'dataset': self.dataset,
                'ranges': ranges
            }
        }

    def path(self, *segments: str):
        return os.path.join(self.state_dir, *segments)
