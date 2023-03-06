import json

import pyarrow

from .tables import BlockTableBuilder, LogTableBuilder, TxTableBuilder, TraceTableBuilder
from .model import Block
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
        self.trace_table = TraceTableBuilder()

    def append(self, data: str):
        block: Block = json.loads(data)

        self.size += len(data)
        self.block_table.append(block['header'])

        for tx in block['transactions']:
            self.tx_table.append(tx)

        for log in block['logs']:
            self.log_table.append(log)

        for trace in block['traces']:
            self.trace_table.append(trace)

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
        traces = self.trace_table.to_table()

        block_numbers: pyarrow.ChunkedArray = blocks.column('number')
        first_block = block_numbers[0].as_py()
        last_block = block_numbers[-1].as_py()

        transactions = transactions.sort_by([('to', 'ascending'), ('sighash', 'ascending')])
        logs = logs.sort_by([('address', 'ascending'), ('topic0', 'ascending')])

        with self.chunk_writer.write(first_block, last_block) as loc:
            kwargs = {
                'data_page_size': 32 * 1024,
                'compression': 'zstd',
                'compression_level': 16
            }
            loc.write_parquet(
                'logs.parquet',
                logs,
                use_dictionary=['address', 'topic0'],
                row_group_size=15000,
                **kwargs
            )
            loc.write_parquet(
                'transactions.parquet',
                transactions,
                use_dictionary=['to', 'sighash'],
                row_group_size=15000,
                **kwargs
            )
            loc.write_parquet(
                'traces.parquet',
                traces,
                **kwargs
            )
            loc.write_parquet(
                'blocks.parquet',
                blocks,
                use_dictionary=False,
                **kwargs
            )
