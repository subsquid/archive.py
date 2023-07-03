import logging
from typing import NamedTuple

import pyarrow

from etha.fs import Fs
from etha.ingest.model import Block
from etha.ingest.tables import BlockTableBuilder, LogTableBuilder, TxTableBuilder, TraceTableBuilder, \
    StateDiffTableBuilder
from etha.ingest.util import short_hash
from etha.layout import ChunkWriter


LOG = logging.getLogger(__name__)


class ArrowDataBatch(NamedTuple):
    blocks: pyarrow.Table
    transactions: pyarrow.Table
    logs: pyarrow.Table
    traces: pyarrow.Table
    statediffs: pyarrow.Table
    bytesize: int


class ArrowBatchBuilder:
    def __init__(self):
        self._init()

    def _init(self):
        self.block_table = BlockTableBuilder()
        self.tx_table = TxTableBuilder()
        self.log_table = LogTableBuilder()
        self.trace_table = TraceTableBuilder()
        self.statediff_table = StateDiffTableBuilder()

    def buffered_bytes(self) -> int:
        return self.block_table.bytesize() \
            + self.tx_table.bytesize() \
            + self.log_table.bytesize() \
            + self.trace_table.bytesize() \
            + self.statediff_table.bytesize()

    def append(self, block: Block):
        self.block_table.append(block)

        for tx in block['transactions']:
            self.tx_table.append(tx)

            if frame := tx.get('debugFrame_'):
                self.trace_table.debug_append(tx['blockNumber'], tx['transactionIndex'], frame['result'])
            elif trace := tx.get('traceReplay_', {}).get('trace'):
                self.trace_table.trace_append(tx['blockNumber'], tx['transactionIndex'], trace)

            if diff := tx.get('debugStateDiff_'):
                self.statediff_table.debug_append(tx['blockNumber'], tx['transactionIndex'], diff['result'])
            elif diff := tx.get('traceReplay_', {}).get('stateDiff'):
                self.statediff_table.trace_append(tx['blockNumber'], tx['transactionIndex'], diff)

        if 'logs_' in block:
            for log in block['logs_']:
                self.log_table.append(log)
        else:
            for tx in block['transactions']:
                if receipt := tx.get('receipt_'):
                    for log in receipt['logs']:
                        self.log_table.append(log)

    def build(self) -> ArrowDataBatch:
        bytesize = self.buffered_bytes()
        batch = ArrowDataBatch(
            blocks=self.block_table.to_table(),
            transactions=self.tx_table.to_table(),
            logs=self.log_table.to_table(),
            traces=self.trace_table.to_table(),
            statediffs=self.statediff_table.to_table(),
            bytesize=bytesize
        )
        self._init()
        return batch


class ParquetWriter:
    def __init__(self, fs: Fs, chunk_writer: ChunkWriter, with_traces: bool, with_statediffs: bool):
        self.fs = fs
        self.chunk_writer = chunk_writer
        self.with_traces = with_traces
        self.with_statediffs = with_statediffs

    def write(self, batch: ArrowDataBatch) -> None:
        blocks = batch.blocks
        transactions = batch.transactions
        logs = batch.logs
        traces = batch.traces
        statediffs = batch.statediffs

        block_numbers: pyarrow.ChunkedArray = blocks.column('number')
        first_block = block_numbers[0].as_py()
        last_block = block_numbers[-1].as_py()
        last_hash = short_hash(blocks.column('hash')[-1].as_py())

        extra = {'first_block': first_block, 'last_block': last_block, 'last_hash': last_hash}
        LOG.debug('saving data chunk', extra=extra)

        transactions = transactions.sort_by([
            ('to', 'ascending'),
            ('sighash', 'ascending')
        ])

        logs = logs.sort_by([
            ('address', 'ascending'),
            ('topic0', 'ascending')
        ])

        chunk = self.chunk_writer.next_chunk(first_block, last_block, last_hash)

        with self.fs.transact(chunk.path()) as loc:
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

            LOG.debug('wrote %s', loc.abs('logs.parquet'))

            loc.write_parquet(
                'transactions.parquet',
                transactions,
                use_dictionary=['to', 'sighash'],
                row_group_size=15000,
                **kwargs
            )

            LOG.debug('wrote %s', loc.abs('transactions.parquet'))

            if self.with_traces:
                loc.write_parquet(
                    'traces.parquet',
                    traces,
                    use_dictionary=[
                        'type',
                        'call_from',
                        'call_to',
                        'call_type'
                    ],
                    row_group_size=15000,
                    **kwargs
                )

                LOG.debug('wrote %s', loc.abs('traces.parquet'))

            if self.with_statediffs:
                loc.write_parquet(
                    'statediffs.parquet',
                    statediffs,
                    use_dictionary=[
                        'address',
                        'kind'
                    ],
                    row_group_size=15000,
                    **kwargs
                )

                LOG.debug('wrote %s', loc.abs('statediffs.parquet'))

            loc.write_parquet(
                'blocks.parquet',
                blocks,
                use_dictionary=False,
                **kwargs
            )

            LOG.debug('wrote %s', loc.abs('blocks.parquet'))
