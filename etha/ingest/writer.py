from typing import NamedTuple

import pyarrow

from .model import Block
from .tables import BlockTableBuilder, LogTableBuilder, TxTableBuilder, TraceTableBuilder
from ..layout import ChunkWriter


class DataBatch(NamedTuple):
    blocks: pyarrow.Table
    transactions: pyarrow.Table
    logs: pyarrow.Table
    traces: pyarrow.Table
    bytesize: int


class BatchBuilder:
    def __init__(self):
        self._init()

    def _init(self):
        self.block_table = BlockTableBuilder()
        self.tx_table = TxTableBuilder()
        self.log_table = LogTableBuilder()
        self.trace_table = TraceTableBuilder()

    def buffered_bytes(self) -> int:
        return self.block_table.bytesize() \
            + self.tx_table.bytesize() \
            + self.log_table.bytesize() \
            + self.trace_table.bytesize()

    def append(self, block: Block):
        self.block_table.append(block)

        for tx in block['transactions']:
            self.tx_table.append(tx)

        for log in block.get('logs_', []):
            self.log_table.append(log)

        for trace in block.get('trace_', []):
            self.trace_table.append(trace)

    def build(self) -> DataBatch:
        bytesize = self.buffered_bytes()
        batch = DataBatch(
            blocks=self.block_table.to_table(),
            transactions=self.tx_table.to_table(),
            logs=self.log_table.to_table(),
            traces=self.trace_table.to_table(),
            bytesize=bytesize
        )
        self._init()
        return batch


class Writer:
    def __init__(self, chunk_writer: ChunkWriter, with_traces: bool):
        self.chunk_writer = chunk_writer
        self.with_traces = with_traces

    def write(self, batch: DataBatch) -> None:
        blocks = batch.blocks
        transactions = batch.transactions
        logs = batch.logs
        traces = batch.traces

        block_numbers: pyarrow.ChunkedArray = blocks.column('number')
        first_block = block_numbers[0].as_py()
        last_block = block_numbers[-1].as_py()

        transactions = transactions.sort_by([('to', 'ascending'), ('sighash', 'ascending')])
        logs = logs.sort_by([('address', 'ascending'), ('topic0', 'ascending')])

        with self.chunk_writer.write(first_block, last_block) as loc:
            kwargs = {
                'data_page_size': 32 * 1024,
                'compression': 'zstd',
                'compression_level': 12
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

            if self.with_traces:
                loc.write_parquet(
                    'traces.parquet',
                    traces,
                    use_dictionary=[
                        'type',
                        'create_from',
                        'call_from',
                        'call_to',
                        'call_type',
                        'reward_author',
                        'reward_type'
                    ],
                    row_group_size=15000,
                    **kwargs
                )

            loc.write_parquet(
                'blocks.parquet',
                blocks,
                use_dictionary=False,
                **kwargs
            )
