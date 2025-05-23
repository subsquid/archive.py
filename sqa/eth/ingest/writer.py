import logging
from typing import TypedDict, NotRequired
import tempfile
import json
import os
import time

import pyarrow

from sqa.eth.ingest.model import Block
from sqa.eth.ingest.tables import BlockTableBuilder, LogTableBuilder, TxTableBuilder, TraceTableBuilder, \
    StateDiffTableBuilder
from sqa.eth.ingest.util import short_hash
from sqa.eth.ingest.metadata import generate_metadata
from sqa.fs import Fs
from sqa.layout import ChunkWriter
from sqa.writer.parquet import add_size_column, add_index_column


LOG = logging.getLogger(__name__)


class ArrowDataBatch(TypedDict):
    blocks: pyarrow.Table
    transactions: pyarrow.Table
    logs: pyarrow.Table
    traces: NotRequired[pyarrow.Table]
    statediffs: NotRequired[pyarrow.Table]
    bytesize: int


class ArrowBatchBuilder:
    def __init__(self, with_traces: bool, with_statediffs: bool):
        self._with_traces = with_traces
        self._with_statediffs = with_statediffs
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

            if self._with_traces:
                if frame := tx.get('debugFrame_'):
                    self.trace_table.debug_append(tx['blockNumber'], tx['transactionIndex'], frame['result'])
                elif trace := tx.get('traceReplay_', {}).get('trace'):
                    self.trace_table.trace_append(tx['blockNumber'], tx['transactionIndex'], trace)

            if self._with_statediffs:
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
            bytesize=bytesize
        )

        if self._with_traces:
            batch['traces'] = self.trace_table.to_table()

        if self._with_statediffs:
            batch['statediffs'] = self.statediff_table.to_table()

        self._init()
        return batch


class ParquetWriter:
    def __init__(
        self,
        fs: Fs,
        chunk_writer: ChunkWriter,
        with_traces: bool,
        with_statediffs: bool,
        with_metadata: bool,
        metrics=None
    ):
        self.fs = fs
        self.chunk_writer = chunk_writer
        self.with_traces = with_traces
        self.with_statediffs = with_statediffs
        self.with_metadata = with_metadata
        self.metrics = metrics
    def write(self, batch: ArrowDataBatch) -> None:
        blocks = batch['blocks']
        block_numbers: pyarrow.ChunkedArray = blocks.column('number')
        first_block = block_numbers[0].as_py()
        last_block = block_numbers[-1].as_py()
        last_hash = short_hash(blocks.column('hash')[-1].as_py())

        chunk = self.chunk_writer.next_chunk(first_block, last_block, last_hash)
        LOG.debug('saving data chunk %s', chunk.path())

        with self.fs.transact(chunk.path()) as loc:
            if self.with_metadata:
                write_metadata(loc, self.with_traces, self.with_statediffs)
            write_parquet(loc, batch)
            
        if self.metrics:
            last_block_timestamp = blocks.column('timestamp')[-1].as_py()
            if hasattr(last_block_timestamp, 'timestamp'):
                last_block_timestamp = int(last_block_timestamp.timestamp())
            current_time = int(time.time())
            processing_time = current_time - last_block_timestamp
            
            self.metrics.set_processing_metrics(last_block_timestamp, processing_time)
            LOG.debug(f'Processed block {last_block} with timestamp {last_block_timestamp}, processing time: {processing_time}s')


def write_metadata(loc: Fs, with_traces: bool, with_statediffs: bool):
    tmp = tempfile.NamedTemporaryFile(mode='w', delete=False)
    try:
        metadata = generate_metadata(with_traces, with_statediffs)
        with tmp:
            json.dump(metadata, tmp, indent=2)
        loc.upload(tmp.name, 'metadata.json')
    finally:
        os.remove(tmp.name)

    LOG.debug('wrote %s', loc.abs('metadata.json'))


def write_parquet(loc: Fs, batch: ArrowDataBatch) -> None:
    kwargs = {
        'data_page_size': 32 * 1024,
        'dictionary_pagesize_limit': 128 * 1024,
        'compression': 'zstd',
        # 'compression_level': 12,
        'write_page_index': True,
        'write_batch_size': 100
    }

    transactions = batch['transactions']
    transactions = transactions.sort_by([
        ('sighash', 'ascending'),
        ('to', 'ascending'),
        ('from', 'ascending'),
        ('block_number', 'ascending'),
        ('transaction_index', 'ascending')
    ])
    transactions = add_size_column(transactions, 'input')
    transactions = add_index_column(transactions)

    loc.write_parquet(
        'transactions.parquet',
        transactions,
        use_dictionary=['from', 'to', 'sighash'],
        row_group_size=10000,
        write_statistics=['_idx', 'from', 'to', 'sighash', 'block_number', 'transaction_index'],
        **kwargs
    )

    LOG.debug('wrote %s', loc.abs('transactions.parquet'))

    logs = batch['logs']
    logs = logs.sort_by([
        ('topic0', 'ascending'),
        ('address', 'ascending'),
        ('block_number', 'ascending'),
        ('log_index', 'ascending')
    ])
    logs = add_size_column(logs, 'data')
    logs = add_index_column(logs)

    loc.write_parquet(
        'logs.parquet',
        logs,
        use_dictionary=['address', 'topic0'],
        row_group_size=10000,
        write_statistics=['_idx', 'address', 'topic0', 'block_number', 'transaction_index', 'log_index'],
        **kwargs
    )

    LOG.debug('wrote %s', loc.abs('logs.parquet'))

    if 'traces' in batch:
        traces = batch['traces']
        traces = traces.sort_by([
            ('type', 'ascending'),
            ('create_from', 'ascending'),
            ('call_sighash', 'ascending'),
            ('call_to', 'ascending'),
            ('call_from', 'ascending'),
            ('block_number', 'ascending'),
            ('transaction_index', 'ascending')
        ])
        traces = add_size_column(traces, 'create_init')
        traces = add_size_column(traces, 'create_result_code')
        traces = add_size_column(traces, 'call_input')
        traces = add_size_column(traces, 'call_result_output')
        traces = add_index_column(traces)

        loc.write_parquet(
            'traces.parquet',
            traces,
            use_dictionary=[
                'type',
                'call_from',
                'call_to',
                'call_type',
                'call_sighash'
            ],
            write_statistics=[
                '_idx',
                'type',
                'call_from',
                'call_to',
                'call_sighash',
                'block_number',
                'transaction_index'
            ],
            row_group_size=10000,
            **kwargs
        )

        LOG.debug('wrote %s', loc.abs('traces.parquet'))

    if 'statediffs' in batch:
        statediffs = batch['statediffs']
        statediffs = add_size_column(statediffs, 'prev')
        statediffs = add_size_column(statediffs, 'next')
        statediffs = add_index_column(statediffs)

        loc.write_parquet(
            'statediffs.parquet',
            statediffs,
            use_dictionary=[
                'address',
                'kind'
            ],
            write_statistics=['_idx', 'block_number', 'transaction_index', 'address', 'key'],
            row_group_size=10000,
            **kwargs
        )

    blocks = batch['blocks']
    blocks = add_size_column(blocks, 'extra_data')

    loc.write_parquet(
        'blocks.parquet',
        blocks,
        write_statistics=['number'],
        row_group_size=2000,
        **kwargs
    )

    LOG.debug('wrote %s', loc.abs('blocks.parquet'))
