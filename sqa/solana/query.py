from typing import TypedDict, Literal, Iterable

import marshmallow as mm
import pyarrow

from sqa.query.model import Table, Item, Scan, ReqName, JoinRel, RefRel
from sqa.query.schema import field_map_schema, BaseQuerySchema
from sqa.query.util import get_selected_fields, json_project, field_in, to_snake_case
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
    error: bool
    d8: bool
    d16: bool
    d32: bool
    d64: bool


class LogFieldSelection(TypedDict, total=False):
    programId: bool
    kind: bool
    message: bool


class FieldSelection(TypedDict, total=False):
    block: BlockFieldSelection
    transaction: TransactionFieldSelection
    instruction: InstructionFieldSelection
    log: LogFieldSelection


class _FieldSelectionSchema(mm.Schema):
    block = field_map_schema(BlockFieldSelection)
    transaction = field_map_schema(TransactionFieldSelection)
    instruction = field_map_schema(InstructionFieldSelection)
    log = field_map_schema(LogFieldSelection)


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
    d8: list[int]
    d16: list[int]
    d32: list[int]
    d64: list[int]
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
    transaction: bool
    logs: bool


class _InstructionRequestSchema(mm.Schema):
    programId = mm.fields.List(mm.fields.Str())
    d8 = mm.fields.List(mm.fields.Integer())
    d16 = mm.fields.List(mm.fields.Integer())
    d32 = mm.fields.List(mm.fields.Integer())
    d64 = mm.fields.List(mm.fields.Integer())
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
    transaction = mm.fields.Boolean()
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


class _QuerySchema(BaseQuerySchema):
    fields = mm.fields.Nested(_FieldSelectionSchema())
    transactions = mm.fields.List(mm.fields.Nested(_TransactionRequestSchema()))
    instructions = mm.fields.List(mm.fields.Nested(_InstructionRequestSchema()))
    logs = mm.fields.List(mm.fields.Nested(_LogRequestSchema()))


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
        def rewrite_timestamp(f: str):
            if f == 'timestamp':
                return 'timestamp', f'epoch_ms(timestamp)'
            else:
                return f

        return json_project(
            map(rewrite_timestamp, self.get_selected_fields(fields))
        )


_transactions_table = Table(
    name='transactions',
    primary_key=['index']
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
        return get_selected_fields(fields.get('transaction'), ['index'])


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
        yield field_in('d8', req.get('d8'))
        yield field_in('d16', req.get('d16'))
        yield field_in('d32', req.get('d32'))
        yield field_in('d64', req.get('d64'))
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
        def rewrite_accounts(f: str):
            if f == 'accounts':
                return 'accounts', f'[a for a in list_value(a0, a1, a2, a3, a4, a5, a6, a7, a8, a9) if a is not null]'
            else:
                return f

        return json_project(
            map(rewrite_accounts, self.get_selected_fields(fields))
        )


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


def _build_model():
    tx_scan = _TransactionScan()
    ins_scan = _InstructionScan()
    log_scan = _LogScan()

    block_item = _BlockItem()
    tx_item = _TransactionItem()
    ins_item = _InstructionItem()
    log_item = _LogItem()

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
        RefRel(
            scan=log_scan,
            include_flag_name='instruction',
            scan_columns=['transaction_index', 'instruction_address']
        )
    ])

    log_item.sources.extend([
        log_scan,
        JoinRel(
            scan=tx_scan,
            include_flag_name='logs',
            query='SELECT * FROM logs i, s WHERE '
                  'i.block_number = s.block_number AND '
                  'i.transaction_index = s.index'
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

    return [
        tx_scan,
        ins_scan,
        log_scan,
        block_item,
        tx_item,
        ins_item,
        log_item
    ]


MODEL = _build_model()
