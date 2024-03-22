import json

import base58
import pyarrow

from sqa.fs import Fs
from sqa.writer.parquet import TableBuilder, Column, BaseParquetWriter, add_index_column, add_size_column
from .model import BlockHeader, Transaction, Instruction, Block, LogMessage, Balance, TokenBalance, Reward


def base58_bytes():
    return pyarrow.string()


def address():
    return pyarrow.list_(pyarrow.uint32())


def JSON():
    return pyarrow.string()


class BlockTable(TableBuilder):
    def __init__(self):
        self.number = Column(pyarrow.int32())
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
        self.block_number = Column(pyarrow.int32())
        self.transaction_index = Column(pyarrow.int32())
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
        self.has_dropped_log_messages = Column(pyarrow.bool_())
        # index
        self.fee_payer = Column(base58_bytes())
        # sizes
        self.account_keys_size = Column(pyarrow.int64())
        self.address_table_lookups_size = Column(pyarrow.int64())
        self.signatures_size = Column(pyarrow.int64())
        self.loaded_addresses_size = Column(pyarrow.int64())

    def append(self, block_number: int, tx: Transaction) -> None:
        self.block_number.append(block_number)
        self.transaction_index.append(tx['transactionIndex'])
        self.version.append(-1 if tx['version'] == 'legacy' else tx['version'])

        account_keys = tx['accountKeys']
        self.account_keys.append(account_keys)
        self.account_keys_size.append(_list_size(account_keys))

        address_table_lookups = tx['addressTableLookups']
        self.address_table_lookups.append(address_table_lookups)
        self.address_table_lookups_size.append(sum(
            len(t['accountKey']) + len(t['readonlyIndexes']) + len(t['writableIndexes'])
            for t in address_table_lookups
        ))

        self.num_readonly_signed_accounts.append(tx['numReadonlySignedAccounts'])
        self.num_readonly_unsigned_accounts.append(tx['numReadonlyUnsignedAccounts'])
        self.num_required_signatures.append(tx['numRequiredSignatures'])
        self.recent_block_hash.append(tx['recentBlockhash'])

        signatures = tx['signatures']
        self.signatures.append(signatures)
        self.signatures_size.append(_list_size(signatures))

        err = tx.get('err')
        self.err.append(None if err is None else json.dumps(err))

        self.compute_units_consumed.append(_to_int(tx.get('computeUnitsConsumed')))
        self.fee.append(int(tx['fee']))

        loaded_addresses = tx['loadedAddresses']
        self.loaded_addresses.append(loaded_addresses)
        self.loaded_addresses_size.append(
            _list_size(loaded_addresses['writable']) + _list_size(loaded_addresses['readonly'])
        )

        self.has_dropped_log_messages.append(tx['hasDroppedLogMessages'])

        self.fee_payer.append(tx['accountKeys'][0])


class InstructionTable(TableBuilder):
    def __init__(self):
        self.block_number = Column(pyarrow.int32())
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
        self.a10 = Column(base58_bytes())
        self.a11 = Column(base58_bytes())
        self.a12 = Column(base58_bytes())
        self.a13 = Column(base58_bytes())
        self.a14 = Column(base58_bytes())
        self.a15 = Column(base58_bytes())
        self.rest_accounts = Column(pyarrow.list_(base58_bytes()))
        self.data = Column(base58_bytes())

        # meta
        self.compute_units_consumed = Column(pyarrow.uint64())
        self.error = Column(pyarrow.string())
        self.is_committed = Column(pyarrow.bool_())
        self.has_dropped_log_messages = Column(pyarrow.bool_())

        # discriminators
        self.d1 = Column(pyarrow.string())
        self.d2 = Column(pyarrow.string())
        self.d4 = Column(pyarrow.string())
        self.d8 = Column(pyarrow.string())

        self.accounts_size = Column(pyarrow.int64())

    def append(self, block_number: int, i: Instruction) -> None:
        self.block_number.append(block_number)
        self.transaction_index.append(i['transactionIndex'])
        self.instruction_address.append(i['instructionAddress'])
        self.program_id.append(i['programId'])
        self._set_accounts(i['accounts'])
        self.data.append(i['data'])

        self.compute_units_consumed.append(_to_int(i.get('computeUnitsConsumed')))
        self.error.append(i.get('error'))
        self.is_committed.append(i['isCommitted'])
        self.has_dropped_log_messages.append(i['hasDroppedLogMessages'])

        data = base58.b58decode(i['data'])
        self.d1.append(f'0x{data[:1].hex()}')
        self.d2.append(f'0x{data[:2].hex()}')
        self.d4.append(f'0x{data[:4].hex()}')
        self.d8.append(f'0x{data[:8].hex()}')

    def _set_accounts(self, accounts: list[str]) -> None:
        for i in range(16):
            col = getattr(self, f'a{i}')
            col.append(accounts[i] if i < len(accounts) else None)
        if len(accounts) > 16:
            self.rest_accounts.append(accounts[16:])
        else:
            self.rest_accounts.append(None)
        self.accounts_size.append(_list_size(accounts))


class LogTable(TableBuilder):
    def __init__(self):
        self.block_number = Column(pyarrow.int32())
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


class BalanceTable(TableBuilder):
    def __init__(self):
        self.block_number = Column(pyarrow.int32())
        self.transaction_index = Column(pyarrow.int32())
        self.account = Column(base58_bytes())
        self.pre = Column(pyarrow.uint64())
        self.post = Column(pyarrow.uint64())

    def append(self, block_number: int, b: Balance) -> None:
        self.block_number.append(block_number)
        self.transaction_index.append(b['transactionIndex'])
        self.account.append(b['account'])
        self.pre.append(int(b['pre']))
        self.post.append(int(b['post']))


class TokenBalanceTable(TableBuilder):
    def __init__(self):
        self.block_number = Column(pyarrow.int32())
        self.transaction_index = Column(pyarrow.int32())
        self.account = Column(base58_bytes())
        self.mint = Column(base58_bytes())
        self.decimals = Column(pyarrow.uint16())
        self.program_id = Column(base58_bytes())
        self.pre_owner = Column(base58_bytes())
        self.post_owner = Column(base58_bytes())
        self.pre = Column(pyarrow.uint64())
        self.post = Column(pyarrow.uint64())

    def append(self, block_number: int, b: TokenBalance) -> None:
        self.block_number.append(block_number)
        self.transaction_index.append(b['transactionIndex'])
        self.account.append(b['account'])
        self.mint.append(b['mint'])
        self.decimals.append(b['decimals'])
        self.program_id.append(b.get('programId'))
        self.pre_owner.append(b.get('preOwner'))
        self.post_owner.append(b.get('postOwner'))
        self.pre.append(_to_int(b.get('pre')))
        self.post.append(_to_int(b.get('post')))


class RewardTable(TableBuilder):
    def __init__(self):
        self.block_number = Column(pyarrow.int32())
        self.pubkey = Column(base58_bytes())
        self.lamports = Column(pyarrow.int64())
        self.post_balance = Column(pyarrow.uint64())
        self.reward_type = Column(pyarrow.string())
        self.commission = Column(pyarrow.uint8())

    def append(self, block_number: int, r: Reward) -> None:
        self.block_number.append(block_number)
        self.pubkey.append(r['pubkey'])
        self.lamports.append(int(r['lamports']))
        self.post_balance.append(int(r['postBalance']))
        self.reward_type.append(r.get('rewardType'))
        self.commission.append(r.get('commission'))


class ParquetWriter(BaseParquetWriter):
    def __init__(self):
        self.blocks = BlockTable()
        self.transactions = TransactionTable()
        self.instructions = InstructionTable()
        self.logs = LogTable()
        self.balances = BalanceTable()
        self.token_balances = TokenBalanceTable()
        self.rewards = RewardTable()

    def push(self, block: Block) -> None:
        block_number = block['header']['height']

        self.blocks.append(block['header'])

        for tx in block['transactions']:
            self.transactions.append(block_number, tx)

        for i in block['instructions']:
            self.instructions.append(block_number, i)

        for msg in block['logs']:
            self.logs.append(block_number, msg)

        for b in block['balances']:
            self.balances.append(block_number, b)

        for b in block['tokenBalances']:
            self.token_balances.append(block_number, b)

        for r in block['rewards']:
            self.rewards.append(block_number, r)

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
        'data_page_size': 32 * 1024,
        'dictionary_pagesize_limit': 192 * 1024,
        'compression': 'zstd',
        'write_page_index': True,
        'write_batch_size': 50
    }

    transactions = tables['transactions']
    transactions = transactions.sort_by([
        ('fee_payer', 'ascending'),
        ('block_number', 'ascending'),
        ('transaction_index', 'ascending'),
    ])
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
            '_idx',
            'fee_payer',
            'block_number',
            'transaction_index',
            'has_dropped_log_messages'
        ],
        row_group_size=5_000,
        **kwargs
    )

    instructions = tables['instructions']
    instructions = instructions.sort_by([
        ('d1', 'ascending'),
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
            'a0', 'a1', 'a2', 'a3', 'a4', 'a5', 'a6', 'a7', 'a8', 'a9', 'a10', 'a11', 'a12', 'a13', 'a14', 'a15',
            'rest_accounts.list.element',
            'd1'
        ],
        write_statistics=[
            '_idx',
            'block_number',
            'transaction_index',
            'program_id',
            'a0', 'a1', 'a2', 'a3', 'a4', 'a5', 'a6', 'a7', 'a8', 'a9', 'a10', 'a11', 'a12', 'a13', 'a14', 'a15',
            'd1', 'd2', 'd4', 'd8',
            'has_dropped_log_messages'
        ],
        row_group_size=20_000,
        **kwargs
    )

    logs = tables['logs']
    logs = logs.sort_by([
        ('program_id', 'ascending'),
        ('kind', 'ascending'),
        ('block_number', 'ascending'),
        ('log_index', 'ascending')
    ])
    logs = add_size_column(logs, 'message')
    logs = add_index_column(logs)

    fs.write_parquet(
        'logs.parquet',
        logs,
        use_dictionary=['program_id', 'kind'],
        write_statistics=[
            '_idx',
            'block_number',
            'transaction_index',
            'log_index',
            'program_id',
            'kind'
        ],
        row_group_size=50_000,
        **kwargs
    )

    balances = tables['balances']
    balances = balances.sort_by([
        ('account', 'ascending'),
        ('block_number', 'ascending'),
        ('transaction_index', 'ascending'),
    ])
    balances = add_index_column(balances)

    fs.write_parquet(
        'balances.parquet',
        balances,
        use_dictionary=['account'],
        write_statistics=['_idx', 'account', 'block_number', 'transaction_index'],
        row_group_size=20_000,
        **kwargs
    )

    token_balances = tables['token_balances']
    token_balances = token_balances.sort_by([
        ('program_id', 'ascending'),
        ('mint', 'ascending'),
        ('account', 'ascending'),
        ('block_number', 'ascending'),
        ('transaction_index', 'ascending'),
    ])
    token_balances = add_index_column(token_balances)

    fs.write_parquet(
        'token_balances.parquet',
        token_balances,
        use_dictionary=[
            'account',
            'mint',
            'pre_owner',
            'post_owner',
            'program_id'
        ],
        write_statistics=[
            '_idx',
            'account',
            'mint',
            'pre_owner',
            'post_owner',
            'program_id',
            'block_number',
            'transaction_index'
        ],
        row_group_size=8_000,
        **kwargs
    )

    rewards = tables['rewards']
    rewards = rewards.sort_by([
        ('pubkey', 'ascending'),
        ('block_number', 'ascending'),
    ])
    rewards = add_index_column(rewards)

    fs.write_parquet(
        'rewards.parquet',
        rewards,
        use_dictionary=['pubkey', 'reward_type'],
        write_statistics=['_idx', 'pubkey', 'reward_type', 'block_number'],
        **kwargs
    )

    blocks = tables['blocks']

    fs.write_parquet(
        'blocks.parquet',
        blocks,
        **kwargs
    )


def _list_size(ls: list[str]) -> int:
    return sum(len(i) for i in ls)


def _to_int(val: str | None) -> int | None:
    return None if val is None else int(val)
