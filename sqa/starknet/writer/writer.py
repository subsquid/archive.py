import logging

import pyarrow

from sqa.fs import Fs
from sqa.starknet.writer.model import WriterBlock
from sqa.starknet.writer.tables import BlockTableBuilder, EventTableBuilder, StateUpdateTableBuilder, TraceTableBuilder, TxTableBuilder
from sqa.writer.parquet import BaseParquetWriter, add_size_column, add_index_column


LOG = logging.getLogger(__name__)


class ParquetWriter(BaseParquetWriter):
    def __init__(self):
        self.blocks = BlockTableBuilder()
        self.transactions = TxTableBuilder()
        self.events = EventTableBuilder()
        self.traces = TraceTableBuilder()
        self.state_updates = StateUpdateTableBuilder()

    def push(self, block: WriterBlock) -> None:
        self.blocks.append(block)

        for tx in block['transactions']:
            self.transactions.append(tx)

        for trace in block.get('traces', []):
            self.traces.append_root(trace)

        if 'writer_state_update' in block:
            self.state_updates.append(block['writer_state_update'])

        if 'writer_events' in block:
            for event in block['writer_events']:
                self.events.append(event)
        else:
            raise NotImplementedError('Receipts not implemented')

    def _write(self, fs: Fs, tables: dict[str, pyarrow.Table]) -> None:
        write_parquet(fs, tables)

    def get_block_height(self, block: WriterBlock) -> int:  # type: ignore
        return block['number']

    def get_block_hash(self, block: WriterBlock) -> str:  # type: ignore
        return block['hash']

    def get_block_parent_hash(self, block: WriterBlock) -> str:  # type: ignore
        return block['parent_hash']


def write_parquet(loc: Fs, tables: dict[str, pyarrow.Table]) -> None:
    kwargs = {
        'data_page_size': 128 * 1024,
        'dictionary_pagesize_limit': 256 * 1024,
        'compression': 'zstd',
        'write_page_index': True,
        'write_batch_size': 100
    }

    # Handling Starknet transactions
    transactions = tables['transactions']
    transactions = transactions.sort_by([
        ('contract_address', 'ascending'),
        ('sender_address', 'ascending'),
        ('block_number', 'ascending'),
        ('transaction_index', 'ascending'),
    ])
    transactions = add_size_column(transactions, 'calldata')
    transactions = add_size_column(transactions, 'signature')
    transactions = add_size_column(transactions, 'constructor_calldata')
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
            'transaction_index'
        ],
        **kwargs
    )

    LOG.debug('wrote %s', loc.abs('transactions.parquet'))

    # Handling Starknet events
    events = tables['events']
    events = events.sort_by([
        ('key0', 'ascending'),
        ('from_address', 'ascending'),
        ('block_number', 'ascending'),
        ('transaction_index', 'ascending'),
        ('event_index', 'ascending'),
    ])
    events = add_size_column(events, 'data')
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
            'event_index'
        ],
        **kwargs
    )

    LOG.debug('wrote %s', loc.abs('events.parquet'))

    # Handling Starknet traces
    traces = tables['traces']
    traces = traces.sort_by([
        ('block_number', 'ascending'),
        ('transaction_index', 'ascending'),
        ('call_index', 'ascending'),
    ])
    traces = add_size_column(traces, 'calldata')
    traces = add_size_column(traces, 'call_result')

    traces = add_size_column(traces, 'call_events_keys')
    traces = add_size_column(traces, 'call_events_data')
    traces = add_size_column(traces, 'call_events_order')

    traces = add_size_column(traces, 'call_messages_payload')
    traces = add_size_column(traces, 'call_messages_from_address')
    traces = add_size_column(traces, 'call_messages_to_address')
    traces = add_size_column(traces, 'call_messages_order')

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
            'call_index',
            'parent_index',
        ],
        **kwargs
    )

    LOG.debug('wrote %s', loc.abs('traces.parquet'))

    # Handling Starknet state updates
    state_updates = tables['state_updates']
    state_updates = state_updates.sort_by([
        ('block_number', 'ascending'),
    ])
    state_updates = add_size_column(state_updates, 'storage_diffs_address')
    state_updates = add_size_column(state_updates, 'storage_diffs_keys')
    state_updates = add_size_column(state_updates, 'storage_diffs_values')
    state_updates = add_size_column(state_updates, 'deprecated_declared_classes')
    state_updates = add_size_column(state_updates, 'declared_classes_class_hash')
    state_updates = add_size_column(state_updates, 'declared_classes_compiled_class_hash')
    state_updates = add_size_column(state_updates, 'deployed_contracts_address')
    state_updates = add_size_column(state_updates, 'deployed_contracts_class_hash')
    state_updates = add_size_column(state_updates, 'replaced_classes_contract_address')
    state_updates = add_size_column(state_updates, 'replaced_classes_class_hash')
    state_updates = add_size_column(state_updates, 'nonces_contract_address')
    state_updates = add_size_column(state_updates, 'nonces_nonce')
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
        **kwargs
    )

    LOG.debug('wrote %s', loc.abs('state_updates.parquet'))

    # Handling Starknet blocks
    blocks = tables['blocks']
    blocks = blocks.sort_by([
        ('number', 'ascending')
    ])

    loc.write_parquet(
        'blocks.parquet',
        blocks,
        use_dictionary=[
            'status',
            'sequencer_address',
            'starknet_version'
        ],
        write_statistics=['number'],
        **kwargs
    )

    LOG.debug('wrote %s', loc.abs('blocks.parquet'))
