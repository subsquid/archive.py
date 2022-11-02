import json

import pyarrow

from .tables import BlockTableBuilder, LogTableBuilder, TxTableBuilder
from ..layout import ChunkWriter


class Writer:
    def __init__(self, chunk_writer: ChunkWriter):
        self.chunk_writer = chunk_writer
        self._init()

    def _init(self):
        self.size = 0
        self.block_table = BlockTableBuilder()
        self.tx_table = TxTableBuilder()
        self.log_table = LogTableBuilder()

    def append(self, json_line: str):
        block = json.loads(json_line)

        self.size += len(json_line)
        self.block_table.append(block['header'])

        for tx in block['transactions']:
            self.tx_table.append(tx)

        for log in block['logs']:
            self.log_table.append(log)

        if self.size > 512 * 1024 * 1024:
            self.flush()

    def flush(self):
        if self.size > 0:
            self._write()
            self._init()

    def _write(self):
        blocks = self.block_table.to_table()
        transactions = self.tx_table.to_table()
        logs = self.log_table.to_table()

        block_numbers: pyarrow.ChunkedArray = blocks.column('number')
        first_block = block_numbers[0].as_py()
        last_block = block_numbers[-1].as_py()

        with self.chunk_writer.write(first_block, last_block) as loc:
            loc.write_parquet('blocks.parquet', blocks, compression='gzip')
            loc.write_parquet('transactions.parquet', transactions, compression='gzip')
            loc.write_parquet('logs.parquet', logs, compression='gzip')
