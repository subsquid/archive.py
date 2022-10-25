import json
import os

import pyarrow
import pyarrow.parquet as pa

from tables import BlockTableBuilder, LogTableBuilder, TxTableBuilder


class Writer:
    def __init__(self, target_dir):
        self.target_dir = target_dir
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
            self._write_pkg()
            self._init()

    def _write_pkg(self):
        blocks = self.block_table.to_table()
        transactions = self.tx_table.to_table()
        logs = self.log_table.to_table()

        block_numbers: pyarrow.ChunkedArray = blocks.column('number')
        first_block = block_numbers[0].as_py()
        last_block = block_numbers[len(block_numbers) - 1].as_py()
        folder_name = f"{first_block:010d}-{last_block:010d}"
        temp_folder = self._item(f"temp_{folder_name}")

        os.makedirs(temp_folder)

        def save_table(name, table):
            pa.write_table(table, os.path.join(temp_folder, f"{name}.parquet"), compression='gzip')

        save_table('blocks', blocks)
        save_table('transactions', transactions)
        save_table('logs', logs)

        os.rename(temp_folder, self._item(folder_name))

    def _item(self, *names):
        return os.path.join(self.target_dir, *names)
