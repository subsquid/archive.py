import struct

import base58
import pyarrow

from sqa.fs import Fs
from sqa.writer.parquet import TableBuilder, Column, BaseParquetWriter, add_index_column, add_size_column
from .model import BlockHeader, Transaction, Instruction, Block


def base58_bytes():
    return pyarrow.string()


def address():
    return pyarrow.list_(pyarrow.uint32())


def JSON():
    return pyarrow.string()


class BlockTable(TableBuilder):
    def __init__(self):
        self.number = Column(pyarrow.int64())
        self.slot = Column(pyarrow.int64())
        self.hash = Column(base58_bytes())
        self.parent_slot = Column(pyarrow.int64())
        self.parent_hash = Column(base58_bytes())
        self.timestamp = Column(pyarrow.timestamp('s', tz='UTC'))

    def append(self, block: BlockHeader) -> None:
        self.number.append(block['height'])
        self.slot.append(block['slot'])
        self.hash.append(block['hash'])
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
        self.log_messages = Column(pyarrow.list_(pyarrow.string()))
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
        self.err.append(tx.get('err'))
        self.compute_units_consumed.append(tx.get('computeUnitsConsumed'))
        self.fee.append(tx['fee'])
        self.log_messages.append(tx['logMessages'])
        self.loaded_addresses.append(tx['loadedAddresses'])
        self.fee_payer.append(tx['accountKeys'][0])


class InstructionTable(TableBuilder):
    def __init__(self):
        self.block_number = Column(pyarrow.int64())
        self.transaction_index = Column(pyarrow.int32())
        self.instruction_address = Column(address())
        self.program_id = Column(base58_bytes())
        self.accounts = Column(pyarrow.list_(base58_bytes()))
        self.data = Column(base58_bytes())
        # discriminators
        self.d8 = Column(pyarrow.uint8())
        self.d16 = Column(pyarrow.uint16())
        self.d32 = Column(pyarrow.uint32())
        self.d64 = Column(pyarrow.uint64())

    def append(self, block_number: int, i: Instruction) -> None:
        self.block_number.append(block_number)
        self.transaction_index.append(i['transactionIndex'])
        self.instruction_address.append(i['instructionAddress'])
        self.program_id.append(i['programId'])
        self.accounts.append(i['accounts'])
        self.data.append(i['data'])

        data = base58.b58decode(i['data'])
        self.d8.append(data[0])
        self.d16.append(struct.unpack_from('<H', data) if len(data) >= 2 else 0)
        self.d32.append(struct.unpack_from('<I', data) if len(data) >= 4 else 0)
        self.d64.append(struct.unpack_from('<Q', data) if len(data) >= 8 else 0)


class ParquetWriter(BaseParquetWriter):
    def __init__(self):
        self.blocks = BlockTable()
        self.transactions = TransactionTable()
        self.instructions = InstructionTable()

    def push(self, block: Block) -> None:
        block_number = block['header']['height']

        self.blocks.append(block['header'])

        for tx in block['transactions']:
            self.transactions.append(block_number, tx)

        for i in block['instructions']:
            self.instructions.append(block_number, i)

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
        'dictionary_pagesize_limit': 128 * 1024,
        'compression': 'zstd',
        'write_page_index': True,
        'write_batch_size': 100
    }

    transactions = tables['transactions']
    transactions = add_index_column(transactions)

    fs.write_parquet(
        'transactions.parquet',
        transactions,
        **kwargs
    )

    instructions = tables['instructions']
    instructions = instructions.sort_by([
        ('program_id', 'ascending'),
        ('block_number', 'ascending'),
        ('transaction_index', 'ascending'),
        ('instruction_address', 'ascending')
    ])
    instructions = add_size_column(instructions, 'data')
    instructions = add_index_column(instructions)

    fs.write_parquet(
        'instructions.parquet',
        instructions,
        use_dictionary=['program_id'],
        row_group_size=100_000_000,
        **kwargs
    )

    blocks = tables['blocks']

    fs.write_parquet(
        'blocks.parquet',
        blocks,
        **kwargs
    )
