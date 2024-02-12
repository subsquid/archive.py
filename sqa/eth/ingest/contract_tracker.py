import logging
import csv
import gzip
import tempfile
import os
import concurrent.futures

from sqa.eth.ingest.model import Block, Address20
from sqa.eth.ingest.util import traverse_frame
from sqa.fs import Fs, LocalFs
from sqa.layout import DataChunk, get_chunks


LOG = logging.getLogger(__name__)


def write_new_contracts(fs: Fs, new_contracts: dict[Address20, Address20]):
    if new_contracts:
        tmp = tempfile.NamedTemporaryFile(delete=False)

        try:
            gzip_file = gzip.open(tmp, "wt")
            csv_writer = csv.writer(gzip_file)
            csv_writer.writerows(new_contracts.items())
            gzip_file.close()
            tmp.close()

            if isinstance(fs, LocalFs):
                dest = fs.abs('new_contracts.csv.gz')
                os.makedirs(os.path.dirname(dest), exist_ok=True)
                os.rename(tmp.name, dest)
            else:
                fs.upload(tmp.name, 'new_contracts.csv.gz')
        finally:
            try:
                os.remove(tmp.name)
            except FileNotFoundError:
                pass

        LOG.debug('wrote %s', fs.abs('new_contracts.csv.gz'))
    else:
        LOG.debug('no new contracts to write')


def load_new_contracts(fs: Fs, chunk: DataChunk) -> dict[Address20, Address20]:
    contracts = {}
    filename = f'{chunk.path()}/new_contracts.csv.gz' 
    file = fs.open(filename, 'rb')
    gzip_file = gzip.open(file, 'rt')

    csv_reader = csv.reader(gzip_file)
    for new_address, parent_address in csv_reader:
        contracts[new_address] = parent_address

    LOG.debug('read %s', fs.abs(filename))

    return contracts


class ContractTracker:
    def __init__(self, fs: Fs):
        self._fs = fs
        self._contracts = {}
        self._new_contracts = {}

    def init(self):
        chunks = tuple(get_chunks(self._fs))

        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(load_new_contracts, self._fs, chunk) for chunk in chunks]

            for future in concurrent.futures.as_completed(futures):
                try:
                    chunk_contracts = future.result()
                except FileNotFoundError:
                    continue
                else:
                    self._contracts.update(chunk_contracts)

    def update(self, block: Block):
        for tx in block['transactions']:
            if frame := tx.get('debugFrame_'):
                for _, _, frame in traverse_frame(frame['result'], []):
                    frame_type = frame['type']
                    if frame_type in ('CREATE', 'CREATE2', 'Create'):
                        if address := frame.get('to'):
                            self._new_contracts[address] = frame['from']
            elif trace := tx.get('traceReplay_', {}).get('trace'):
                for record in trace:
                    if record['type'] == 'create':
                        if result := record.get('result'):
                            self._new_contracts[result['address']] = record['action']['from']

    def get_new_contracts(self):
        new_contracts = self._new_contracts
        self._new_contracts = {}
        return new_contracts

    def get_parents(self, address: Address20) -> list[Address20]:
        parents = []
        while parent := self._contracts.get(address):
            parents.append(parent)
            address = parent
        return parents
