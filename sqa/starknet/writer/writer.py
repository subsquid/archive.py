import logging
from typing import TypedDict, NotRequired

import pyarrow

from sqa.starknet.writer.model import Block, WriterBlock
from sqa.starknet.writer.tables import BlockTableBuilder, EventTableBuilder, TxTableBuilder
from sqa.eth.ingest.util import short_hash
from sqa.fs import Fs
from sqa.layout import ChunkWriter
from sqa.writer.parquet import BaseParquetWriter, add_size_column, add_index_column


LOG = logging.getLogger(__name__)
    

class ParquetWriter(BaseParquetWriter):
    def __init__(self):
        self.blocks = BlockTableBuilder()
        self.transactions = TxTableBuilder()
        self.events = EventTableBuilder()

    def push(self, block: WriterBlock) -> None:
        # TODO: add functionality from eth writer
        self.blocks.append(block)

        for tx in block['transactions']:
            self.transactions.append(tx)

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
        'dictionary_pagesize_limit': 2 * 1024 * 1024,
        'compression': 'zstd',
        'write_page_index': True,
        'write_batch_size': 100
    }

    # Handling Starknet transactions
    transactions = tables['transactions']
    transactions = transactions.sort_by([
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
        use_dictionary=['contract_address', 'transaction_hash', 'sender_address'],
        row_group_size=10000,
        write_statistics=['_idx', 'block_number', 'transaction_index'],
        **kwargs
    )

    LOG.debug('wrote %s', loc.abs('transactions.parquet'))

    # Handling Starknet events
    events = tables['events']
    events = events.sort_by([
        ('block_number', 'ascending'),
        ('transaction_index', 'ascending'),
        ('event_index', 'ascending'),
    ])
    events = add_size_column(events, 'keys')
    events = add_size_column(events, 'data')
    events = add_index_column(events)

    loc.write_parquet(
        'events.parquet',
        events,
        use_dictionary=['from_address', 'transaction_hash'],
        row_group_size=10000,
        write_statistics=['_idx', 'block_number', 'transaction_index', 'event_index'],
        **kwargs
    )

    LOG.debug('wrote %s', loc.abs('events.parquet'))

    # Handling Starknet blocks
    blocks = tables['blocks']

    loc.write_parquet(
        'blocks.parquet',
        blocks,
        use_dictionary=['hash'],
        write_statistics=['number'],
        row_group_size=2000,
        **kwargs
    )

    LOG.debug('wrote %s', loc.abs('blocks.parquet'))

    # TODO: Add processing for traces, and state diffs
