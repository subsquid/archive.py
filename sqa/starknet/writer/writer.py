import logging
from typing import TypedDict, NotRequired

import pyarrow

from sqa.starknet.writer.model import Block
from sqa.starknet.writer.tables import BlockTableBuilder, LogTableBuilder, TxTableBuilder
from sqa.eth.ingest.util import short_hash
from sqa.eth.ingest.writer import ArrowDataBatch
from sqa.fs import Fs
from sqa.layout import ChunkWriter
from sqa.writer.parquet import add_size_column, add_index_column


LOG = logging.getLogger(__name__)


class ArrowBatchBuilder:
    def __init__(self, with_traces: bool, with_statediffs: bool):
        self._with_traces = with_traces
        self._with_statediffs = with_statediffs
        self._init()

    def _init(self):
        self.block_table = BlockTableBuilder()
        self.tx_table = TxTableBuilder()
        self.log_table = LogTableBuilder()
        # self.trace_table = TraceTableBuilder()
        # self.statediff_table = StateDiffTableBuilder()

    def buffered_bytes(self) -> int:
        return self.block_table.bytesize() \
            + self.tx_table.bytesize() \
            + self.log_table.bytesize() \
            # + self.trace_table.bytesize() \
            # + self.statediff_table.bytesize()
    
    def append(self, block: Block):
        # TODO: add functionality from eth writer
        self.block_table.append(block)

        for tx in block['transactions']:
            self.tx_table.append(tx)

        if 'logs_' in block:
            for log in block['logs_']:
                self.log_table.append(log)
        else:
            raise NotImplementedError('Receipts not implemented')

    def build(self) -> ArrowDataBatch:
        bytesize = self.buffered_bytes()

        batch = ArrowDataBatch(
            blocks=self.block_table.to_table(),
            transactions=self.tx_table.to_table(),
            logs=self.log_table.to_table(),
            bytesize=bytesize
        )

        # if self._with_traces:
        #     batch['traces'] = self.trace_table.to_table()

        # if self._with_statediffs:
        #     batch['statediffs'] = self.statediff_table.to_table()

        self._init()
        return batch


class ParquetWriter:
    def __init__(self, fs: Fs, chunk_writer: ChunkWriter, with_traces: bool, with_statediffs: bool):
        self.fs = fs
        self.chunk_writer = chunk_writer
        if with_traces or with_statediffs:
            raise NotImplementedError('traces and statediffs are not implemented for starknet')
        self.with_traces = with_traces
        self.with_statediffs = with_statediffs

    def write(self, batch: ArrowDataBatch) -> None:
        blocks = batch['blocks']
        block_numbers: pyarrow.ChunkedArray = blocks.column('number') # NOTE: block_number
        first_block = block_numbers[0].as_py()
        last_block = block_numbers[-1].as_py()
        last_hash = short_hash(blocks.column('block_hash')[-1].as_py())

        chunk = self.chunk_writer.next_chunk(first_block, last_block, last_hash)
        LOG.debug('saving data chunk %s', chunk.path())

        with self.fs.transact(chunk.path()) as loc:
            write_parquet(loc, batch)

def write_parquet(loc: Fs, batch: ArrowDataBatch) -> None:
    kwargs = {
        'data_page_size': 128 * 1024,
        'dictionary_pagesize_limit': 2 * 1024 * 1024,
        'compression': 'zstd',
        'write_page_index': True,
        'write_batch_size': 100
    }

    # Handling Starknet transactions
    transactions = batch['transactions']
    transactions = add_size_column(transactions, 'calldata')
    transactions = add_index_column(transactions)

    loc.write_parquet(
        'transactions.parquet',
        transactions,
        use_dictionary=['contract_address', 'entry_point_selector'],
        row_group_size=10000,
        write_statistics=['_idx', 'block_number'],
        **kwargs
    )

    LOG.debug('wrote %s', loc.abs('transactions.parquet'))

    # Handling Starknet logs(events)
    logs = batch['logs']
    logs = logs.sort_by([
        ('from_address', 'ascending'),
        ('block_number', 'ascending')
    ])
    # TODO: maybe add size columns for data and keys arrays
    logs = add_index_column(logs)

    loc.write_parquet(
        'logs.parquet',
        logs,
        use_dictionary=['from_address'],
        row_group_size=10000,
        write_statistics=['_idx', 'from_address', 'block_number'],
        **kwargs
    )

    LOG.debug('wrote %s', loc.abs('logs.parquet'))

    # Handling Starknet blocks
    blocks = batch['blocks']

    loc.write_parquet(
        'blocks.parquet',
        blocks,
        write_statistics=['number'], # NOTE: block_number
        row_group_size=2000,
        **kwargs
    )

    LOG.debug('wrote %s', loc.abs('blocks.parquet'))

    # TODO: Add processing for logs, traces, and state diffs
