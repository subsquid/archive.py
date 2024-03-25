from typing import TypedDict, Literal, Iterable

import marshmallow as mm
import pyarrow

from sqa.query.model import Table, Item, Scan, ReqName, JoinRel, RefRel
from sqa.query.schema import field_map_schema, BaseQuerySchema
from sqa.query.util import get_selected_fields, json_project, field_in, to_snake_case, field_eq
from sqa.solana.writer.model import Base58Bytes


class BlockFieldSelection(TypedDict, total=False):
    slot: bool
    parentSlot: bool
    parentHash: bool
    timestamp: bool


class TransactionFieldSelection(TypedDict, total=False):
    version: bool
    accountKeys: bool
    addressTableLookups: bool
    numReadonlySignedAccounts: bool
    numReadonlyUnsignedAccounts: bool
    numRequiredSignatures: bool
    recentBlockhash: bool
    signatures: bool
    err: bool
    fee: bool
    computeUnitsConsumed: bool
    loadedAddresses: bool
    feePayer: bool


class InstructionFieldSelection(TypedDict, total=False):
    programId: bool
    accounts: bool
    data: bool
    d1: bool
    d2: bool
    d4: bool
    d8: bool
    error: bool
    computeUnitsConsumed: bool
    isCommitted: bool


class LogFieldSelection(TypedDict, total=False):
    instructionAddress: bool
    programId: bool
    kind: bool
    message: bool


class BalanceFieldSelection(TypedDict, total=False):
    pre: bool
    post: bool


class TokenBalanceFieldSelection(TypedDict, total=False):
    mint: bool
    owner: bool
    programId: bool
    decimals: bool
    pre: bool
    post: bool


class RewardFieldSelection(TypedDict, total=False):
    lamports: bool
    postBalance: bool
    rewardType: bool
    commission: bool


class FieldSelection(TypedDict, total=False):
    block: BlockFieldSelection
    transaction: TransactionFieldSelection
    instruction: InstructionFieldSelection
    log: LogFieldSelection
    balance: BalanceFieldSelection
    tokenBalance: TokenBalanceFieldSelection
    reward: RewardFieldSelection


class _FieldSelectionSchema(mm.Schema):
    block = field_map_schema(BlockFieldSelection)
    transaction = field_map_schema(TransactionFieldSelection)
    instruction = field_map_schema(InstructionFieldSelection)
    log = field_map_schema(LogFieldSelection)
    balance = field_map_schema(BalanceFieldSelection)
    tokenBalance = field_map_schema(TokenBalanceFieldSelection)
    reward = field_map_schema(RewardFieldSelection)


class TransactionRequest(TypedDict, total=False):
    feePayer: list[Base58Bytes]
    instructions: bool
    logs: bool


class _TransactionRequestSchema(mm.Schema):
    feePayer = mm.fields.List(mm.fields.Str())
    instructions = mm.fields.Boolean()
    logs = mm.fields.Boolean()


class InstructionRequest(TypedDict, total=False):
    programId: list[Base58Bytes]
    d1: list[str]
    d2: list[str]
    d4: list[str]
    d8: list[str]
    a0: list[Base58Bytes]
    a1: list[Base58Bytes]
    a2: list[Base58Bytes]
    a3: list[Base58Bytes]
    a4: list[Base58Bytes]
    a5: list[Base58Bytes]
    a6: list[Base58Bytes]
    a7: list[Base58Bytes]
    a8: list[Base58Bytes]
    a9: list[Base58Bytes]
    isCommitted: bool
    transaction: bool
    transactionTokenBalances: bool
    innerInstructions: bool
    logs: bool


class _InstructionRequestSchema(mm.Schema):
    programId = mm.fields.List(mm.fields.Str())
    d1 = mm.fields.List(mm.fields.Str())
    d2 = mm.fields.List(mm.fields.Str())
    d4 = mm.fields.List(mm.fields.Str())
    d8 = mm.fields.List(mm.fields.Str())
    a0 = mm.fields.List(mm.fields.Str())
    a1 = mm.fields.List(mm.fields.Str())
    a2 = mm.fields.List(mm.fields.Str())
    a3 = mm.fields.List(mm.fields.Str())
    a4 = mm.fields.List(mm.fields.Str())
    a5 = mm.fields.List(mm.fields.Str())
    a6 = mm.fields.List(mm.fields.Str())
    a7 = mm.fields.List(mm.fields.Str())
    a8 = mm.fields.List(mm.fields.Str())
    a9 = mm.fields.List(mm.fields.Str())
    isCommitted = mm.fields.Boolean()
    transaction = mm.fields.Boolean()
    transactionTokenBalances = mm.fields.Boolean()
    innerInstructions = mm.fields.Boolean()
    logs = mm.fields.Boolean()


class LogRequest(TypedDict, total=False):
    programId: list[Base58Bytes]
    kind: list[Literal['log', 'data', 'other']]
    instruction: bool
    transaction: bool


class _LogRequestSchema(mm.Schema):
    programId = mm.fields.List(mm.fields.Str())
    kind = mm.fields.List(mm.fields.Str())
    transaction = mm.fields.Boolean()
    instruction = mm.fields.Boolean()


class BalanceRequest(TypedDict, total=False):
    account: list[Base58Bytes]
    transaction: bool
    transactionInstructions: bool


class _BalanceRequestSchema(mm.Schema):
    account = mm.fields.List(mm.fields.Str())
    transaction = mm.fields.Boolean()
    transactionInstructions = mm.fields.Boolean()


class TokenBalanceRequest(TypedDict, total=False):
    account: list[Base58Bytes]
    mint: list[Base58Bytes]
    owner: list[Base58Bytes]
    programId: list[Base58Bytes]
    transaction: bool
    transactionInstructions: bool


class _TokenBalanceRequestSchema(mm.Schema):
    account = mm.fields.List(mm.fields.Str())
    mint = mm.fields.List(mm.fields.Str())
    owner = mm.fields.List(mm.fields.Str())
    programId = mm.fields.List(mm.fields.Str())
    transaction = mm.fields.Boolean()
    transactionInstructions = mm.fields.Boolean()


class RewardRequest(TypedDict, total=False):
    pubkey: list[Base58Bytes]


class _RewardRequestSchema(mm.Schema):
    pubkey = mm.fields.List(mm.fields.Str())


class _QuerySchema(BaseQuerySchema):
    fields = mm.fields.Nested(_FieldSelectionSchema())
    transactions = mm.fields.List(mm.fields.Nested(_TransactionRequestSchema()))
    instructions = mm.fields.List(mm.fields.Nested(_InstructionRequestSchema()))
    logs = mm.fields.List(mm.fields.Nested(_LogRequestSchema()))
    balances = mm.fields.List(mm.fields.Nested(_BalanceRequestSchema()))
    tokenBalances = mm.fields.List(mm.fields.Nested(_TokenBalanceRequestSchema()))
    rewards = mm.fields.List(mm.fields.Nested(_RewardRequestSchema()))


QUERY_SCHEMA = _QuerySchema()


_blocks_table = Table(
    name='blocks',
    primary_key=[]
)


class _BlockItem(Item):
    def table(self) -> Table:
        return _blocks_table

    def name(self) -> str:
        return 'blocks'

    def get_selected_fields(self, fields: FieldSelection) -> list[str]:
        return get_selected_fields(fields.get('block'), ['number', 'hash'])

    def project(self, fields: FieldSelection) -> str:
        return json_project(self.get_selected_fields(fields), rewrite={
            'timestamp': 'epoch_ms(timestamp)'
        })


_transactions_table = Table(
    name='transactions',
    primary_key=['transaction_index'],
    column_weights={
        'account_keys': 'account_keys_size',
        'address_table_lookups': 'address_table_lookups_size',
        'signatures': 'signatures_size',
        'loaded_addresses': 'loaded_addresses_size'
    }
)


class _TransactionScan(Scan):
    def table(self) -> Table:
        return _transactions_table

    def request_name(self) -> str:
        return 'transactions'

    def where(self, req: TransactionRequest) -> Iterable[pyarrow.dataset.Expression | None]:
        yield field_in('fee_payer', req.get('feePayer'))


class _TransactionItem(Item):
    def table(self) -> Table:
        return _transactions_table

    def name(self) -> str:
        return 'transactions'

    def get_selected_fields(self, fields: FieldSelection) -> list[str]:
        return get_selected_fields(fields.get('transaction'), ['transactionIndex'])

    def project(self, fields: FieldSelection) -> str:
        return json_project(self.get_selected_fields(fields), rewrite={
            'version': "if(version < 0, 'legacy'::json, version::json)",
            'computeUnitsConsumed': 'compute_units_consumed::text',
            'fee': 'fee::text',
            'err': 'err::json'
        })


_instructions_table = Table(
    name='instructions',
    primary_key=['transaction_index', 'instruction_address'],
    column_weights={
        'data': 'data_size',
        'a0': 'accounts_size',  # hack, put all accounts weight to a single column
        'a1': 0,
        'a2': 0,
        'a3': 0,
        'a4': 0,
        'a5': 0,
        'a6': 0,
        'a7': 0,
        'a8': 0,
        'a9': 0,
        'rest_accounts': 0,
    }
)


class _InstructionScan(Scan):
    def table(self) -> Table:
        return _instructions_table

    def request_name(self) -> ReqName:
        return 'instructions'

    def where(self, req: InstructionRequest) -> Iterable[pyarrow.dataset.Expression | None]:
        yield field_in('program_id', req.get('programId'))
        yield field_in('d1', req.get('d1'))
        yield field_in('d2', req.get('d2'))
        yield field_in('d4', req.get('d4'))
        yield field_in('d8', req.get('d8'))
        yield field_in('a0', req.get('a0'))
        yield field_in('a1', req.get('a1'))
        yield field_in('a2', req.get('a2'))
        yield field_in('a3', req.get('a3'))
        yield field_in('a4', req.get('a4'))
        yield field_in('a5', req.get('a5'))
        yield field_in('a6', req.get('a6'))
        yield field_in('a7', req.get('a7'))
        yield field_in('a8', req.get('a8'))
        yield field_in('a9', req.get('a9'))
        yield field_eq('is_committed', req.get('isCommitted'))


class _InstructionItem(Item):
    def table(self) -> Table:
        return _instructions_table

    def name(self) -> str:
        return 'instructions'

    def get_selected_fields(self, fields: FieldSelection) -> list[str]:
        return get_selected_fields(fields.get('instruction'), ['transactionIndex', 'instructionAddress'])

    def selected_columns(self, fields: FieldSelection) -> list[str]:
        columns = []
        for name in self.get_selected_fields(fields):
            if name == 'accounts':
                columns.append('a0')
                columns.append('a1')
                columns.append('a2')
                columns.append('a3')
                columns.append('a4')
                columns.append('a5')
                columns.append('a6')
                columns.append('a7')
                columns.append('a8')
                columns.append('a9')
                columns.append('rest_accounts')
            else:
                columns.append(to_snake_case(name))
        return columns

    def project(self, fields: FieldSelection) -> str:
        return json_project(self.get_selected_fields(fields), rewrite={
            'accounts': f'list_concat('
                        f'[a for a in list_value(a0, a1, a2, a3, a4, a5, a6, a7, a8, a9) if a is not null], '
                        f'rest_accounts)'
        })


_logs_table = Table(
    name='logs',
    primary_key=['transaction_index', 'log_index'],
    column_weights={
        'message': 'message_size'
    }
)


class _LogScan(Scan):
    def table(self) -> Table:
        return _logs_table

    def request_name(self) -> ReqName:
        return 'logs'

    def where(self, req: LogRequest) -> Iterable[pyarrow.dataset.Expression | None]:
        yield field_in('program_id', req.get('programId'))


class _LogItem(Item):
    def table(self) -> Table:
        return _logs_table

    def name(self) -> str:
        return 'logs'

    def get_selected_fields(self, fields: FieldSelection) -> list[str]:
        return get_selected_fields(fields.get('log'), ['transactionIndex', 'logIndex'])


_balance_table = Table(
    name='balances',
    primary_key=['transaction_index', 'account']
)


class _BalanceScan(Scan):
    def table(self) -> Table:
        return _balance_table

    def request_name(self) -> ReqName:
        return 'balances'

    def where(self, req: BalanceRequest) -> Iterable[pyarrow.dataset.Expression | None]:
        yield field_in('account', req.get('account'))


class _BalanceItem(Item):
    def table(self) -> Table:
        return _balance_table

    def name(self) -> str:
        return 'balances'

    def get_selected_fields(self, fields: FieldSelection) -> list[str]:
        return get_selected_fields(fields.get('balance'), ['transactionIndex', 'account'])

    def project(self, fields: FieldSelection) -> str:
        return json_project(self.get_selected_fields(fields), rewrite={
            'pre': 'pre::text',
            'post': 'post::text'
        })


_token_balance_table = Table(
    name='token_balances',
    primary_key=[
        'transaction_index',
        'account'
    ]
)


class _TokenBalanceScan(Scan):
    def table(self) -> Table:
        return _token_balance_table

    def request_name(self) -> ReqName:
        return 'tokenBalances'

    def where(self, req: TokenBalanceRequest) -> Iterable[pyarrow.dataset.Expression | None]:
        yield field_in('account', req.get('account'))
        yield field_in('mint', req.get('mint'))
        yield field_in('owner', req.get('owner'))
        yield field_in('programId', req.get('programId'))


class _TokenBalanceItem(Item):
    def table(self) -> Table:
        return _token_balance_table

    def name(self) -> str:
        return 'tokenBalances'

    def get_selected_fields(self, fields: FieldSelection) -> list[str]:
        return get_selected_fields(fields.get('tokenBalance'), ['transactionIndex', 'account'])

    def project(self, fields: FieldSelection) -> str:
        return json_project(self.get_selected_fields(fields), rewrite={
            'pre': 'pre::text',
            'post': 'post::text'
        })


_reward_table = Table(
    name='rewards',
    primary_key=['pubkey']
)


class _RewardScan(Scan):
    def table(self) -> Table:
        return _reward_table

    def request_name(self) -> ReqName:
        return 'rewards'

    def where(self, req: RewardRequest) -> Iterable[pyarrow.dataset.Expression | None]:
        yield field_in('pubkey', req.get('pubkey'))


class _RewardItem(Item):
    def table(self) -> Table:
        return _reward_table

    def name(self) -> str:
        return 'rewards'

    def get_selected_fields(self, fields: FieldSelection) -> list[str]:
        return get_selected_fields(fields.get('reward'), ['pubkey'])

    def project(self, fields: FieldSelection) -> str:
        return json_project(self.get_selected_fields(fields), rewrite={
            'lamports': 'lamports::text',
            'postBalance': 'post_balance::text'
        })


def _build_model():
    tx_scan = _TransactionScan()
    ins_scan = _InstructionScan()
    log_scan = _LogScan()
    balance_scan = _BalanceScan()
    token_balance_scan = _TokenBalanceScan()
    reward_scan = _RewardScan()

    block_item = _BlockItem()
    tx_item = _TransactionItem()
    ins_item = _InstructionItem()
    log_item = _LogItem()
    balance_item = _BalanceItem()
    token_balance_item = _TokenBalanceItem()
    reward_item = _RewardItem()

    tx_item.sources.extend([
        tx_scan,
        RefRel(
            scan=ins_scan,
            include_flag_name='transaction',
            scan_columns=['transaction_index'],
        ),
        RefRel(
            scan=log_scan,
            include_flag_name='transaction',
            scan_columns=['transaction_index'],
        ),
        RefRel(
            scan=balance_scan,
            include_flag_name='transaction',
            scan_columns=['transaction_index']
        ),
        RefRel(
            scan=token_balance_scan,
            include_flag_name='transaction',
            scan_columns=['transaction_index']
        )
    ])

    ins_item.sources.extend([
        ins_scan,
        JoinRel(
            scan=tx_scan,
            include_flag_name='instructions',
            query='SELECT * FROM instructions i, s WHERE '
                  'i.block_number = s.block_number AND '
                  'i.transaction_index = s.index'
        ),
        JoinRel(
            scan=ins_scan,
            include_flag_name='innerInstructions',
            query='SELECT * FROM instructions i, s WHERE '
                  'i.block_number = s.block_number AND '
                  'i.transaction_index = s.transaction_index AND '
                  'len(i.instruction_address) > len(s.instruction_address) AND '
                  'i.instruction_address[1:len(s.instruction_address)] = s.instruction_address'
        ),
        RefRel(
            scan=log_scan,
            include_flag_name='instruction',
            scan_columns=['transaction_index', 'instruction_address']
        )
    ])

    for s in (balance_scan, token_balance_scan):
        ins_item.sources.append(
            JoinRel(
                scan=s,
                include_flag_name='transactionInstructions',
                query='SELECT * FROM instructions i, s WHERE '
                      'i.block_number = s.block_number AND '
                      'i.transaction_index = s.transaction_index'
            )
        )

    log_item.sources.extend([
        log_scan,
        JoinRel(
            scan=tx_scan,
            include_flag_name='logs',
            query='SELECT * FROM logs i, s WHERE '
                  'i.block_number = s.block_number AND '
                  'i.transaction_index = s.transaction_index'
        ),
        JoinRel(
            scan=ins_scan,
            include_flag_name='logs',
            query='SELECT * FROM logs i, s WHERE '
                  'i.block_number = s.block_number AND '
                  'i.transaction_index = s.transaction_index AND '
                  'i.instruction_address = s.instruction_address'
        )
    ])

    balance_item.sources.extend([
        balance_scan
    ])

    token_balance_item.sources.extend([
        token_balance_scan,
        JoinRel(
            scan=ins_scan,
            include_flag_name='transactionTokenBalances',
            query='SELECT * FROM token_balances i, s WHERE '
                  'i.block_number = s.block_number AND '
                  'i.transaction_index = s.transaction_index '
        )
    ])

    reward_item.sources.extend([
        reward_scan
    ])

    return [
        tx_scan,
        ins_scan,
        log_scan,
        balance_scan,
        token_balance_scan,
        reward_scan,
        block_item,
        tx_item,
        ins_item,
        log_item,
        balance_item,
        token_balance_item,
        reward_item
    ]


MODEL = _build_model()
