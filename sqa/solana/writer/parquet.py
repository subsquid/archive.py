import json
import struct

import base58
import pyarrow

from sqa.fs import Fs
from sqa.writer.parquet import TableBuilder, Column, BaseParquetWriter, add_index_column, add_size_column
from .model import BlockHeader, Transaction, Instruction, Block, LogMessage


def base58_bytes():
    return pyarrow.string()


def address():
    return pyarrow.list_(pyarrow.uint32())


def JSON():
    return pyarrow.string()


class BlockTable(TableBuilder):
    def __init__(self):
        self.number = Column(pyarrow.int64())
        self.hash = Column(base58_bytes())
        self.slot = Column(pyarrow.int64())
        self.parent_slot = Column(pyarrow.int64())
        self.parent_hash = Column(base58_bytes())
        self.timestamp = Column(pyarrow.timestamp('s', tz='UTC'))

    def append(self, block: BlockHeader) -> None:
        self.number.append(block['height'])
        self.hash.append(block['hash'])
        self.slot.append(block['slot'])
        self.parent_slot.append(block['parentSlot'])
        self.parent_hash.append(block['parentHash'])
        self.timestamp.append(block['timestamp'])


class TransactionTable(TableBuilder):
    def __init__(self):
        self.block_number = Column(pyarrow.int64())
        self.index = Column(pyarrow.int32())
        self.version = Column(pyarrow.int16())  # -1 = legacy
        # transaction message
        self.account_keys = Column(pyarrow.list_(base58_bytes()))
        self.address_table_lookups = Column(pyarrow.list_(
            pyarrow.struct([
                ('account_key', base58_bytes()),
                ('readonly_indexes', pyarrow.list_(pyarrow.uint8())),
                ('writable_indexes', pyarrow.list_(pyarrow.uint8())),
            ])
        ))
        self.num_readonly_signed_accounts = Column(pyarrow.uint8())
        self.num_readonly_unsigned_accounts = Column(pyarrow.uint8())
        self.num_required_signatures = Column(pyarrow.uint8())
        self.recent_block_hash = Column(base58_bytes())
        self.signatures = Column(pyarrow.list_(base58_bytes()))
        # meta
        self.err = Column(JSON())
        self.compute_units_consumed = Column(pyarrow.uint64())
        self.fee = Column(pyarrow.uint64())
        self.loaded_addresses = Column(pyarrow.struct([
            ('readonly', pyarrow.list_(base58_bytes())),
            ('writable', pyarrow.list_(base58_bytes()))
        ]))
        # index
        self.fee_payer = Column(base58_bytes())

    def append(self, block_number: int, tx: Transaction) -> None:
        self.block_number.append(block_number)
        self.index.append(tx['index'])
        self.version.append(-1 if tx['version'] == 'legacy' else tx['version'])
        self.account_keys.append(tx['accountKeys'])
        self.address_table_lookups.append(tx['addressTableLookups'])
        self.num_readonly_signed_accounts.append(tx['numReadonlySignedAccounts'])
        self.num_readonly_unsigned_accounts.append(tx['numReadonlyUnsignedAccounts'])
        self.num_required_signatures.append(tx['numRequiredSignatures'])
        self.recent_block_hash.append(tx['recentBlockhash'])
        self.signatures.append(tx['signatures'])
        err = tx.get('err')
        self.err.append(None if err is None else json.dumps(err))
        self.compute_units_consumed.append(tx.get('computeUnitsConsumed'))
        self.fee.append(tx['fee'])
        self.loaded_addresses.append(tx['loadedAddresses'])
        self.fee_payer.append(tx['accountKeys'][0])


class InstructionTable(TableBuilder):
    def __init__(self):
        self.block_number = Column(pyarrow.int64())
        self.transaction_index = Column(pyarrow.int32())
        self.instruction_address = Column(address())
        self.program_id = Column(base58_bytes())
        self.a0 = Column(base58_bytes())
        self.a1 = Column(base58_bytes())
        self.a2 = Column(base58_bytes())
        self.a3 = Column(base58_bytes())
        self.a4 = Column(base58_bytes())
        self.a5 = Column(base58_bytes())
        self.a6 = Column(base58_bytes())
        self.a7 = Column(base58_bytes())
        self.a8 = Column(base58_bytes())
        self.a9 = Column(base58_bytes())
        self.rest_accounts = Column(pyarrow.list_(base58_bytes()))
        self.data = Column(base58_bytes())
        self.error = Column(pyarrow.string())
        # discriminators
        self.d8 = Column(pyarrow.uint8())
        self.d16 = Column(pyarrow.uint16())
        self.d32 = Column(pyarrow.uint32())
        self.d64 = Column(pyarrow.uint64())
        self.accounts_size = Column(pyarrow.int64())

    def append(self, block_number: int, i: Instruction) -> None:
        self.block_number.append(block_number)
        self.transaction_index.append(i['transactionIndex'])
        self.instruction_address.append(i['instructionAddress'])
        self.program_id.append(i['programId'])
        self._set_accounts(i['accounts'])
        self.data.append(i['data'])
        self.error.append(i.get('error'))

        data = base58.b58decode(i['data'])
        self.d8.append(data[0] if data else None)
        self.d16.append(struct.unpack_from('<H', data)[0] if len(data) >= 2 else None)
        self.d32.append(struct.unpack_from('<I', data)[0] if len(data) >= 4 else None)
        self.d64.append(struct.unpack_from('<Q', data)[0] if len(data) >= 8 else None)

    def _set_accounts(self, accounts: list[str]) -> None:
        for i in range(10):
            col = getattr(self, f'a{i}')
            col.append(accounts[i] if i < len(accounts) else None)
        if len(accounts) > 10:
            self.rest_accounts.append(accounts[10:])
        else:
            self.rest_accounts.append(None)
        self.accounts_size.append(sum(len(a) for a in accounts))


class LogTable(TableBuilder):
    def __init__(self):
        self.block_number = Column(pyarrow.int64())
        self.transaction_index = Column(pyarrow.int32())
        self.log_index = Column(pyarrow.int32())
        self.instruction_address = Column(address())
        self.program_id = Column(base58_bytes())
        self.kind = Column(pyarrow.string())
        self.message = Column(pyarrow.string())

    def append(self, block_number: int, log: LogMessage) -> None:
        self.block_number.append(block_number)
        self.transaction_index.append(log['transactionIndex'])
        self.log_index.append(log['logIndex'])
        self.instruction_address.append(log['instructionAddress'])
        self.program_id.append(log['programId'])
        self.kind.append(log['kind'])
        self.message.append(log['message'])


class ParquetWriter(BaseParquetWriter):
    def __init__(self):
        self.blocks = BlockTable()
        self.transactions = TransactionTable()
        self.instructions = InstructionTable()
        self.logs = LogTable()

    def push(self, block: Block) -> None:
        block_number = block['header']['height']

        self.blocks.append(block['header'])

        for tx in block['transactions']:
            self.transactions.append(block_number, tx)

        for i in block['instructions']:
            self.instructions.append(block_number, i)

        for msg in block['logs']:
            self.logs.append(block_number, msg)

    def _write(self, fs: Fs, tables: dict[str, pyarrow.Table]) -> None:
        write_parquet(fs, tables)

    def get_block_height(self, block: Block) -> int:
        return block['header']['height']

    def get_block_hash(self, block: Block) -> str:
        return block['header']['hash']

    def get_block_parent_hash(self, block: Block) -> str:
        return block['header']['parentHash']


def write_parquet(fs: Fs, tables: dict[str, pyarrow.Table]) -> None:
    kwargs = {
        'data_page_size': 128 * 1024,
        'dictionary_pagesize_limit': 2 * 1024 * 1024,
        'compression': 'zstd',
        'write_page_index': True,
        'write_batch_size': 200
    }

    transactions = tables['transactions']
    transactions = add_index_column(transactions)

    fs.write_parquet(
        'transactions.parquet',
        transactions,
        use_dictionary=[
            'account_keys.list.element',
            'address_table_lookups.list.element.account_key',
            'loaded_addresses.readonly.list.element',
            'loaded_addresses.writable.list.element',
            'fee_payer'
        ],
        write_statistics=[
            'block_number',
            'index',
            'fee_payer',
            '_idx'
        ],
        row_group_size=100_000,
        **kwargs
    )

    instructions = tables['instructions']
    instructions = instructions.sort_by([
        ('program_id', 'ascending'),
        ('block_number', 'ascending'),
        ('transaction_index', 'ascending'),
        # ('instruction_address', 'ascending')
    ])
    instructions = add_size_column(instructions, 'data')
    instructions = add_index_column(instructions)

    fs.write_parquet(
        'instructions.parquet',
        instructions,
        use_dictionary=[
            'program_id',
            'a0', 'a1', 'a2', 'a3', 'a4', 'a5', 'a6', 'a7', 'a8', 'a9',
            'rest_accounts.list.element'
        ],
        write_statistics=[
            'block_number',
            'transaction_index',
            'program_id',
            'a0', 'a1', 'a2', 'a3', 'a4', 'a5', 'a6', 'a7', 'a8', 'a9',
            'd8', 'd16', 'd32', 'd64',
            '_idx'
        ],
        row_group_size=100_000,
        **kwargs
    )

    logs = tables['logs']
    logs = add_size_column(logs, 'message')
    logs = add_index_column(logs)

    fs.write_parquet(
        'logs.parquet',
        logs,
        use_dictionary=['program_id'],
        write_statistics=[
            'block_number',
            'transaction_index',
            'log_index',
            'program_id',
            '_idx'
        ],
        row_group_size=100_000,
        **kwargs
    )

    blocks = tables['blocks']

    fs.write_parquet(
        'blocks.parquet',
        blocks,
        **kwargs
    )
