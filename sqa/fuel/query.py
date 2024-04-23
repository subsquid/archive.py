from typing import TypedDict, Literal, Iterable

import marshmallow as mm
import pyarrow

from sqa.query.model import Table, Item, Scan, ReqName, JoinRel, RefRel
from sqa.query.schema import field_map_schema, BaseQuerySchema
from sqa.query.util import get_selected_fields, json_project, field_in, remove_camel_prefix, to_snake_case


class BlockFieldSelection(TypedDict, total=False):
    hash: bool
    height: bool
    daHeight: bool
    transactionsRoot: bool
    transactionsCount: bool
    messageReceiptRoot: bool
    messageReceiptCount: bool
    prevRoot: bool
    time: bool
    applicationHash: bool


class TransactionFieldSelection(TypedDict, total=False):
    index: bool
    hash: bool
    inputAssetIds: bool
    inputContracts: bool
    inputContract: bool
    policies: bool
    gasPrice: bool
    scriptGasLimit: bool
    maturity: bool
    mintAmount: bool
    mintAssetId: bool
    txPointer: bool
    isScript: bool
    isCreate: bool
    isMint: bool
    type: bool
    outputContract: bool
    witnesses: bool
    receiptsRoot: bool
    status: bool
    script: bool
    scriptData: bool
    bytecodeWitnessIndex: bool
    bytecodeLength: bool
    salt: bool
    storageSlots: bool
    rawPayload: bool


ReceiptFieldSelection = TypedDict('ReceiptFieldSelection', {
    'index': bool,
    'transactionIndex': bool,
    'contract': bool,
    'pc': bool,
    'is': bool,
    'to': bool,
    'toAddress': bool,
    'amount': bool,
    'assetId': bool,
    'gas': bool,
    'param1': bool,
    'param2': bool,
    'val': bool,
    'ptr': bool,
    'digest': bool,
    'reason': bool,
    'ra': bool,
    'rb': bool,
    'rc': bool,
    'rd': bool,
    'len': bool,
    'receiptType': bool,
    'result': bool,
    'gasUsed': bool,
    'data': bool,
    'sender': bool,
    'recipient': bool,
    'nonce': bool,
    'contractId': bool,
    'subId': bool,
}, total=False)


class InputFieldSelection(TypedDict, total=False):
    type: bool
    index: bool
    transactionIndex: bool
    coinUtxoId: bool
    coinOwner: bool
    coinAmount: bool
    coinAssetId: bool
    coinTxPointer: bool
    coinWitnessIndex: bool
    coinMaturity: bool
    coinPredicateGasUsed: bool
    coinPredicate: bool
    coinPredicateData: bool
    contractUtxoId: bool
    contractBalanceRoot: bool
    contractStateRoot: bool
    contractTxPointer: bool
    contractContract: bool
    messageSender: bool
    messageRecipient: bool
    messageAmount: bool
    messageNonce: bool
    messageWitnessIndex: bool
    messagePredicateGasUsed: bool
    messageData: bool
    messagePredicate: bool
    messagePredicateData: bool


class OutputFieldSelection(TypedDict, total=False):
    type: bool
    index: bool
    transactionIndex: bool
    coinTo: bool
    coinAmount: bool
    coinAssetId: bool
    contractInputIndex: bool
    contractBalanceRoot: bool
    contractStateRoot: bool
    changeTo: bool
    changeAmount: bool
    changeAssetId: bool
    variableTo: bool
    variableAmount: bool
    variableAssetId: bool
    contractCreatedContractId: bool
    contractCreatedContractBytecode: bool
    contractCreatedContractSalt: bool
    contractCreatedStateRoot: bool


class FieldSelection(TypedDict, total=False):
    block: BlockFieldSelection
    transaction: TransactionFieldSelection
    receipt: ReceiptFieldSelection
    input: InputFieldSelection
    output: OutputFieldSelection


class _FieldSelectionSchema(mm.Schema):
    block = field_map_schema(BlockFieldSelection)
    transaction = field_map_schema(TransactionFieldSelection)
    receipt = field_map_schema(ReceiptFieldSelection)
    input = field_map_schema(InputFieldSelection)
    output = field_map_schema(OutputFieldSelection)


class TransactionRequest(TypedDict, total=False):
    type: list[Literal['Script', 'Create', 'Mint']]
    receipts: bool
    inputs: bool
    outputs: bool


class _TransactionRequestSchema(mm.Schema):
    type = mm.fields.List(mm.fields.Str())
    receipts = mm.fields.Boolean()
    inputs = mm.fields.Boolean()
    outputs = mm.fields.Boolean()


class ReceiptRequest(TypedDict, total=False):
    type: list[Literal['CALL', 'RETURN', 'RETURN_DATA', 'PANIC', 'REVERT', 'LOG', 'LOG_DATA', 'TRANSFER', 'TRANSFER_OUT', 'SCRIPT_RESULT', 'MESSAGE_OUT', 'MINT', 'BURN']]
    contract: list[str]
    transaction: bool


class _ReceiptRequestSchema(mm.Schema):
    type = mm.fields.List(mm.fields.Str())
    contract = mm.fields.List(mm.fields.Str())
    transaction = mm.fields.Boolean()


class InputRequest(TypedDict, total=False):
    type: list[Literal['InputCoin', 'InputContract', 'InputMessage']]
    coinOwner: list[str]
    coinAssetId: list[str]
    contractContract: list[str]
    messageSender: list[str]
    messageRecipient: list[str]
    transaction: bool


class _InputRequestSchema(mm.Schema):
    type = mm.fields.List(mm.fields.Str())
    coinOwner = mm.fields.List(mm.fields.Str())
    coinAssetId = mm.fields.List(mm.fields.Str())
    coinPredicateRoot = mm.fields.List(mm.fields.Str())
    contractContract = mm.fields.List(mm.fields.Str())
    messageSender = mm.fields.List(mm.fields.Str())
    messageRecipient = mm.fields.List(mm.fields.Str())
    messagePredicateRoot = mm.fields.List(mm.fields.Str())
    transaction = mm.fields.Boolean()


class OutputRequest(TypedDict, total=False):
    type: list[Literal['CoinOutput', 'ContractOutput', 'ChangeOutput', 'VariableOutput', 'ContractCreated']]
    transaction: bool


class _OutputRequestSchema(mm.Schema):
    type = mm.fields.List(mm.fields.Str())
    transaction = mm.fields.Boolean()


class _QuerySchema(BaseQuerySchema):
    fields = mm.fields.Nested(_FieldSelectionSchema())
    transactions = mm.fields.List(mm.fields.Nested(_TransactionRequestSchema()))
    receipts = mm.fields.List(mm.fields.Nested(_ReceiptRequestSchema()))
    inputs = mm.fields.List(mm.fields.Nested(_InputRequestSchema()))
    outputs = mm.fields.List(mm.fields.Nested(_OutputRequestSchema()))


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


_transactions_table = Table(
    name='transactions',
    primary_key=['transaction_index'],
    column_weights={
        'input_asset_ids': 'input_asset_ids_size',
        'input_contracts': 'input_contracts_size',
        'witnesses': 'witnesses_size',
        'storage_slots': 'storage_slots_size',
        'script_data': 'script_data_size',
        'raw_payload': 'raw_payload_size'
    }
)


class _TransactionScan(Scan):
    def table(self) -> Table:
        return _transactions_table

    def request_name(self) -> str:
        return 'transactions'

    def where(self, req: TransactionRequest) -> Iterable[pyarrow.dataset.Expression | None]:
        yield field_in('type', req.get('type'))


class _TransactionItem(Item):
    def table(self) -> Table:
        return _transactions_table

    def name(self) -> str:
        return 'transactions'

    def get_selected_fields(self, fields: FieldSelection) -> list[str]:
        return get_selected_fields(fields.get('transaction'), ['transactionIndex'])

    def project(self, fields: FieldSelection) -> str:
        return json_project(self.get_selected_fields(fields), rewrite={
            'gasPrice': 'gas_price::text',
            'scriptGasLimit': 'script_gas_limit::text',
            'mintAmount': 'mint_amount::text',
            'bytecodeLength': 'bytecode_length::text',
            'policies': _POLICIES_PROJECTION,
            'inputContract': _INPUT_CONTRACT_PROJECTION,
            'outputContract': _OUTPUT_CONTRACT
        })


_POLICIES_PROJECTION = '''
    CASE WHEN policies is null THEN null
    ELSE json_object(
        'gasPrice', (policies -> 'gas_price')::text,
        'witnessLimit', (policies -> 'witness_limit')::text,
        'maturity', policies -> 'maturity',
        'maxFee', (policies -> 'max_fee')::text
    ) END
'''


_INPUT_CONTRACT_PROJECTION = '''
    CASE WHEN input_contract is null THEN null
    ELSE json_object(
        'utxoId': input_contract -> 'utxo_id',
        'balanceRoot': input_contract -> 'balance_root',
        'stateRoot': input_contract -> 'state_root',
        'txPointer': input_contract -> 'tx_pointer',
        'contract': input_contract -> 'contract'
    ) END
'''


_OUTPUT_CONTRACT = '''
    CASE WHEN output_contract is null THEN null
    ELSE json_object(
        'inputIndex': output_contract -> 'input_index',
        'balanceRoot': output_contract -> 'balance_root',
        'stateRoot': output_contract -> 'state_root'
    ) END
'''


_receipts_table = Table(
    name='receipts',
    primary_key=['transaction_index', 'index'],
    column_weights={
        'data': 'data_size',
    }
)


class _ReceiptScan(Scan):
    def table(self) -> Table:
        return _receipts_table

    def request_name(self) -> ReqName:
        return 'receipts'

    def where(self, req: ReceiptRequest) -> Iterable[pyarrow.dataset.Expression | None]:
        yield field_in('receipt_type', req.get('type'))
        yield field_in('contract', req.get('contract'))


class _ReceiptItem(Item):
    def table(self) -> Table:
        return _receipts_table

    def name(self) -> str:
        return 'receipts'

    def get_selected_fields(self, fields: FieldSelection) -> list[str]:
        return get_selected_fields(fields.get('receipt'), ['transactionIndex', 'index'])

    def project(self, fields: FieldSelection) -> str:
        return json_project(self.get_selected_fields(fields), rewrite={
            'pc': 'pc::text',
            'is': 'is::text',
            'amount': 'amount::text',
            'gas': 'gas::text',
            'param1': 'param1::text',
            'param2': 'param2::text',
            'val': 'val::text',
            'ptr': 'ptr::text',
            'reason': 'reason::text',
            'ra': 'ra::text',
            'rb': 'rb::text',
            'rc': 'rc::text',
            'rd': 'rd::text',
            'len': 'len::text',
            'result': 'result::text',
            'gasUsed': 'gas_used::text',
        })


_inputs_table = Table(
    name='inputs',
    primary_key=['transaction_index', 'index'],
    column_weights={
        'coinPredicate': 'coin_predicate_size',
        'messagePredicate': 'message_predicate_size',
    }
)


class _InputScan(Scan):
    def table(self) -> Table:
        return _inputs_table

    def request_name(self) -> ReqName:
        return 'inputs'

    def where(self, req: InputRequest) -> Iterable[pyarrow.dataset.Expression | None]:
        yield field_in('type', req.get('type'))
        yield field_in('coin_owner', req.get('coinOwner'))
        yield field_in('coin_asset_id', req.get('coinAssetId'))
        yield field_in('contract_contract', req.get('contractContract'))
        yield field_in('message_sender', req.get('messageSender'))
        yield field_in('message_recipient', req.get('messageRecipient'))


class _InputItem(Item):
    def table(self) -> Table:
        return _inputs_table

    def name(self) -> str:
        return 'inputs'

    def get_selected_fields(self, fields: FieldSelection) -> list[str]:
        return get_selected_fields(fields.get('input'), ['transactionIndex', 'index'])

    def project(self, fields: FieldSelection) -> str:
        selected = self.get_selected_fields(fields)
        proj_fields = []

        for field in selected:
            proj_fields.append(field)
            for prefix in ('coin', 'contract', 'message'):
                if field.startswith(prefix):
                    alias = remove_camel_prefix(field, prefix)
                    exp = to_snake_case(field)
                    if alias in ('amount', 'predicateGasUsed'):
                        exp += '::text'
                    proj_fields[-1] = (alias, exp)
                    break

        return json_project(proj_fields)


_output_table = Table(
    name='outputs',
    primary_key=['transaction_index', 'index'],
    column_weights={
        'contractCreatedContractBytecode': 'contract_created_contract_bytecode_size'
    }
)


class _OutputScan(Scan):
    def table(self) -> Table:
        return _output_table

    def request_name(self) -> ReqName:
        return 'outputs'

    def where(self, req: OutputRequest) -> Iterable[pyarrow.dataset.Expression | None]:
        yield field_in('type', req.get('type'))


class _OutputItem(Item):
    def table(self) -> Table:
        return _output_table

    def name(self) -> str:
        return 'outputs'

    def get_selected_fields(self, fields: FieldSelection) -> list[str]:
        return get_selected_fields(fields.get('output'), ['transactionIndex', 'index'])

    def project(self, fields: FieldSelection) -> str:
        selected = self.get_selected_fields(fields)
        proj_fields = []

        for field in selected:
            proj_fields.append(field)
            for prefix in ('coin', 'contract', 'change', 'variable', 'contractCreated'):
                if field.startswith(prefix):
                    alias = remove_camel_prefix(field, prefix)
                    exp = to_snake_case(field)
                    if alias == 'amount':
                        exp += '::text'
                    proj_fields[-1] = (alias, exp)
                    break

        return json_project(proj_fields)


def _build_model():
    tx_scan = _TransactionScan()
    receipt_scan = _ReceiptScan()
    input_scan = _InputScan()
    output_scan = _OutputScan()

    block_item = _BlockItem()
    tx_item = _TransactionItem()
    receipt_item = _ReceiptItem()
    input_item = _InputItem()
    output_item = _OutputItem()

    tx_item.sources.extend([
        tx_scan,
        RefRel(
            scan=receipt_scan,
            include_flag_name='transaction',
            scan_columns=['transaction_index'],
        ),
        RefRel(
            scan=input_scan,
            include_flag_name='transaction',
            scan_columns=['transaction_index'],
        ),
        RefRel(
            scan=output_scan,
            include_flag_name='transaction',
            scan_columns=['transaction_index']
        )
    ])

    receipt_item.sources.extend([
        receipt_scan,
        JoinRel(
            scan=tx_scan,
            include_flag_name='receipts',
            query='SELECT * FROM receipts i, s WHERE '
                  'i.block_number = s.block_number AND '
                  'i.transaction_index = s.index'
        )
    ])

    input_item.sources.extend([
        input_scan,
        JoinRel(
            scan=tx_scan,
            include_flag_name='inputs',
            query='SELECT * FROM inputs i, s WHERE '
                  'i.block_number = s.block_number AND '
                  'i.transaction_index = s.transaction_index'
        )
    ])

    output_item.sources.extend([
        output_scan,
        JoinRel(
            scan=tx_scan,
            include_flag_name='ouputs',
            query='SELECT * FROM ouputs i, s WHERE '
                  'i.block_number = s.block_number AND '
                  'i.transaction_index = s.transaction_index'
        )
    ])

    return [
        tx_scan,
        receipt_scan,
        input_scan,
        output_scan,
        block_item,
        tx_item,
        receipt_item,
        input_item,
        output_item
    ]


MODEL = _build_model()
