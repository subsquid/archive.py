import logging

import pyarrow

from sqa.fs import Fs
from sqa.starknet.writer.model import WriterBlock
from sqa.starknet.writer.tables import (
    BlockTableBuilder,
    MessageTableBuilder,
    EventTableBuilder,
    StateUpdateTableBuilder,
    StorageDiffTableBuilder,
    TraceTableBuilder,
    TxTableBuilder,
)
from sqa.writer.parquet import BaseParquetWriter, add_index_column

LOG = logging.getLogger(__name__)


class ParquetWriter(BaseParquetWriter):
    def __init__(self) -> None:
        self.blocks = BlockTableBuilder()
        self.transactions = TxTableBuilder()
        self.events = EventTableBuilder()
        self.traces = TraceTableBuilder()
        self.messages = MessageTableBuilder()
        self.state_updates = StateUpdateTableBuilder()
        self.storage_diffs = StorageDiffTableBuilder()

    def push(self, block: WriterBlock) -> None:
        self.blocks.append(block)

        for tx in block['writer_txs']:
            self.transactions.append(tx)

        for trace in block.get('writer_call_traces', []):
            self.traces.append_call(trace)
            self.messages.append_from_call(trace)
            self.events.append_from_trace(trace)

        if 'writer_state_update' in block:
            self.state_updates.append(block['writer_state_update'])

        if 'writer_storage_diffs' in block:
            for storage_diff in block['writer_storage_diffs']:
                self.storage_diffs.append(storage_diff)

        # NOTE: if we can we try to write events from traces with trace address
        if 'writer_events' in block and 'writer_call_traces' not in block:
            for event in block['writer_events']:
                self.events.append(event)

    def _write(self, fs: Fs, tables: dict[str, pyarrow.Table]) -> None:
        write_parquet(fs, tables)

    def get_block_height(self, block: WriterBlock) -> int:  # type: ignore[typeddict-item]
        return block['number']

    def get_block_hash(self, block: WriterBlock) -> str:  # type: ignore[typeddict-item]
        return block['hash']

    def get_block_parent_hash(self, block: WriterBlock) -> str:  # type: ignore[typeddict-item]
        return block['parent_hash']


def write_parquet(loc: Fs, tables: dict[str, pyarrow.Table]) -> None:
    kwargs = {
        'data_page_size': 128 * 1024,
        'dictionary_pagesize_limit': 256 * 1024,
        'compression': 'zstd',
        'write_page_index': True,
        'write_batch_size': 100,
    }

    # Handling Starknet transactions
    transactions = tables['transactions']
    transactions = transactions.sort_by([
        ('contract_address', 'ascending'),
        ('sender_address', 'ascending'),
        ('block_number', 'ascending'),
        ('transaction_index', 'ascending'),
    ])
    transactions = add_index_column(transactions)

    loc.write_parquet(
        'transactions.parquet',
        transactions,
        use_dictionary=['contract_address', 'type', 'version'],
        row_group_size=50000,
        write_statistics=[
            '_idx',
            'contract_address',
            'sender_address',
            'block_number',
            'transaction_index',
        ],
        **kwargs,
    )

    LOG.debug('wrote %s', loc.abs('transactions.parquet'))

    # Handling Starknet events
    events = tables['events']
    events = events.sort_by([
        ('block_number', 'ascending'),
        ('transaction_index', 'ascending'),
        ('event_index', 'ascending'),
        ('key0', 'ascending'),
        ('from_address', 'ascending'),
    ])
    events = add_index_column(events)

    loc.write_parquet(
        'events.parquet',
        events,
        use_dictionary=['key0'],
        row_group_size=100_000,
        write_statistics=[
            '_idx',
            'key0',
            'from_address',
            'block_number',
            'transaction_index',
            'event_index',
        ],
        **kwargs,
    )

    LOG.debug('wrote %s', loc.abs('events.parquet'))

    # Handling Starknet traces
    traces = tables['traces']
    traces = traces.sort_by([
        ('block_number', 'ascending'),
        ('transaction_index', 'ascending'),
    ])

    traces = add_index_column(traces)

    loc.write_parquet(
        'traces.parquet',
        traces,
        use_dictionary=[
            'trace_type',
            'call_type',
            'invocation_type',
        ],
        row_group_size=50_000,
        write_statistics=[
            '_idx',
            'block_number',
            'transaction_index',
        ],
        **kwargs,
    )

    LOG.debug('wrote %s', loc.abs('traces.parquet'))

    # Handling Starknet call messages
    messages = tables['messages']
    messages = messages.sort_by([
        ('block_number', 'ascending'),
        ('transaction_index', 'ascending'),
        ('order', 'ascending'),
    ])
    messages = add_index_column(messages)

    loc.write_parquet(
        'messages.parquet',
        messages,
        use_dictionary=[],
        row_group_size=50_000,
        write_statistics=[
            '_idx',
            'block_number',
            'transaction_index',
        ],
        **kwargs,
    )

    LOG.debug('wrote %s', loc.abs('messages.parquet'))


    # Handling Starknet state updates
    state_updates = tables['state_updates']
    state_updates = state_updates.sort_by([
        ('block_number', 'ascending'),
    ])
    state_updates = add_index_column(state_updates)

    loc.write_parquet(
        'state_updates.parquet',
        state_updates,
        use_dictionary=[],
        row_group_size=100_000,
        write_statistics=[
            '_idx',
            'block_number',
        ],
        **kwargs,
    )

    LOG.debug('wrote %s', loc.abs('state_updates.parquet'))

    # Handling Starknet storage diffs
    storage_diffs = tables['storage_diffs']
    storage_diffs = storage_diffs.sort_by([
        ('block_number', 'ascending'),
        ('address', 'ascending'),
    ])
    storage_diffs = add_index_column(storage_diffs)

    loc.write_parquet(
        'storage_diffs.parquet',
        storage_diffs,
        use_dictionary=[],
        row_group_size=100_000,
        write_statistics=[
            '_idx',
            'block_number',
        ],
        **kwargs,
    )

    LOG.debug('wrote %s', loc.abs('storage_diffs.parquet'))

    # Handling Starknet blocks
    blocks = tables['blocks']
    blocks = blocks.sort_by([
        ('number', 'ascending'),
    ])

    loc.write_parquet(
        'blocks.parquet',
        blocks,
        use_dictionary=[
            'status',
            'sequencer_address',
            'starknet_version',
        ],
        write_statistics=['number'],
        **kwargs,
    )

    LOG.debug('wrote %s', loc.abs('blocks.parquet'))
