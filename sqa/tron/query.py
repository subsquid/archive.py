from typing import TypedDict, Iterable

import marshmallow as mm
from pyarrow.dataset import Expression

from sqa.query.model import JoinRel, RefRel, Table, Item, FieldSelection, Scan
from sqa.query.schema import BaseQuerySchema, field_map_schema
from sqa.query.util import to_snake_case, json_project, get_selected_fields, field_in


class BlockFieldSelection(TypedDict, total=False):
    parentHash: bool
    txTrieRoot: bool
    version: bool
    timestamp: bool
    witnessAddress: bool
    witnessSignature: bool


TxFieldSelection = TypedDict('TxFieldSelection', {
    'hash': bool,
    'ret': bool,
    'signature': bool,
    'type': bool,
    'parameter': bool,
    'permissionId': bool,
    'refBlockBytes': bool,
    'refBlockHash': bool,
    'feeLimit': bool,
    'expiration': bool,
    'timestamp': bool,
    'rawDataHex': bool,
    'fee': bool,
    'contractResult': bool,
    'contractAddress': bool,
    'resMessage': bool,
    'withdrawAmount': bool,
    'unfreezeAmount': bool,
    'withdrawExpireAmount': bool,
    'cancelUnfreezeV2Amount': bool,
    'result': bool,
    'energyFee': bool,
    'energyUsage': bool,
    'energyUsageTotal': bool,
    'netUsage': bool,
    'netFee': bool,
    'originEnergyUsage': bool,
    'energyPenaltyTotal': bool,
}, total=False)


class LogFieldSelection(TypedDict, total=False):
    address: bool
    data: bool
    topics: bool


class InternalTransactionFieldSelection(TypedDict, total=False):
    hash: bool
    callerAddress: bool
    transferToAddress: bool
    callValueInfo: bool
    note: bool
    rejected: bool
    extra: bool


class LogRequest(TypedDict, total=False):
    address: list[str]
    topic0: list[str]
    topic1: list[str]
    topic2: list[str]
    topic3: list[str]
    transaction: bool


class TxRequest(TypedDict, total=False):
    type: list[str]
    logs: bool
    internalTransactions: bool


class TransferTxRequest(TypedDict, total=False):
    owner: list[str]
    to: list[str]
    logs: bool
    internalTransactions: bool


class TransferAssetTxRequest(TypedDict, total=False):
    owner: list[str]
    to: list[str]
    asset: list[str]
    logs: bool
    internalTransactions: bool


class TriggerSmartContractTxRequest(TypedDict, total=False):
    owner: list[str]
    contract: list[str]
    sighash: list[str]
    logs: bool
    internalTransactions: bool


class InternalTxRequest(TypedDict, total=False):
    caller: list[str]
    transferTo: list[str]
    transaction: bool


class _FieldSelectionSchema(mm.Schema):
    block = field_map_schema(BlockFieldSelection)
    log = field_map_schema(LogFieldSelection)
    transaction = field_map_schema(TxFieldSelection)
    internalTransaction = field_map_schema(InternalTransactionFieldSelection)


class _LogRequestSchema(mm.Schema):
    address = mm.fields.List(mm.fields.Str())
    topic0 = mm.fields.List(mm.fields.Str())
    topic1 = mm.fields.List(mm.fields.Str())
    topic2 = mm.fields.List(mm.fields.Str())
    topic3 = mm.fields.List(mm.fields.Str())
    transaction = mm.fields.Boolean()


class _TxRequestSchema(mm.Schema):
    type = mm.fields.List(mm.fields.Str())
    logs = mm.fields.Boolean()
    internalTransactions = mm.fields.Boolean()


class _TransferTxRequestSchema(mm.Schema):
    owner = mm.fields.List(mm.fields.Str())
    to = mm.fields.List(mm.fields.Str())
    logs = mm.fields.Boolean()
    internalTransactions = mm.fields.Boolean()


class _TransferAssetTxRequestSchema(mm.Schema):
    owner = mm.fields.List(mm.fields.Str())
    to = mm.fields.List(mm.fields.Str())
    asset = mm.fields.List(mm.fields.Str())
    logs = mm.fields.Boolean()
    internalTransactions = mm.fields.Boolean()


class _TriggerSmartContractTxRequestSchema(mm.Schema):
    owner = mm.fields.List(mm.fields.Str())
    contract = mm.fields.List(mm.fields.Str())
    sighash = mm.fields.List(mm.fields.Str())
    logs = mm.fields.Boolean()
    internalTransactions = mm.fields.Boolean()


class _InternalTransactionRequestSchema(mm.Schema):
    caller = mm.fields.List(mm.fields.Str())
    transferTo = mm.fields.List(mm.fields.Str())
    transaction = mm.fields.Boolean()


class _QuerySchema(BaseQuerySchema):
    fields = mm.fields.Nested(_FieldSelectionSchema())

    logs = mm.fields.List(mm.fields.Nested(_LogRequestSchema()))

    transactions = mm.fields.List(mm.fields.Nested(_TxRequestSchema()))

    transferTransactions = mm.fields.List(mm.fields.Nested(_TransferTxRequestSchema()))

    transferAssetTransactions = mm.fields.List(mm.fields.Nested(_TransferAssetTxRequestSchema()))

    triggerSmartContractTransactions = mm.fields.List(mm.fields.Nested(_TriggerSmartContractTxRequestSchema()))

    internalTransactions = mm.fields.List(mm.fields.Nested(_InternalTransactionRequestSchema()))


QUERY_SCHEMA = _QuerySchema()


_blocks_table = Table(
    name='blocks',
    primary_key=[]
)


_tx_table = Table(
    name='transactions',
    primary_key=['transaction_index'],
    column_weights={
        'raw_data_hex': 'raw_data_hex_size'
    }
)


_logs_table = Table(
    name='logs',
    primary_key=['transaction_index', 'log_index'],
    column_weights={
        'data': 'data_size'
    }
)


_internal_tx_table = Table(
    name='internal_transactions',
    primary_key=['transaction_index', 'internal_transaction_index']
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


class _TxScan(Scan):
    def table(self) -> Table:
        return _tx_table

    def request_name(self) -> str:
        return 'transactions'

    def where(self, req: TxRequest) -> Iterable[Expression | None]:
        yield field_in('type', req.get('type'))


class _TransferTxScan(Scan):
    def table(self) -> Table:
        return _tx_table

    def request_name(self) -> str:
        return 'transferTransactions'

    def where(self, req: TransferTxRequest) -> Iterable[Expression | None]:
        yield field_in('type', ['TransferContract'])
        yield field_in('_transfer_contract_owner', req.get('owner'))
        yield field_in('_transfer_contract_to', req.get('to'))


class _TransferAssetTxScan(Scan):
    def table(self) -> Table:
        return _tx_table

    def request_name(self) -> str:
        return 'transferAssetTransactions'

    def where(self, req: TransferAssetTxRequest) -> Iterable[Expression | None]:
        yield field_in('type', ['TransferAssetContract'])
        yield field_in('_transfer_asset_contract_owner', req.get('owner'))
        yield field_in('_transfer_asset_contract_to', req.get('to'))
        yield field_in('_transfer_asset_contract_asset', req.get('asset'))


class _TriggerSmartContractTransferTxScan(Scan):
    def table(self) -> Table:
        return _tx_table

    def request_name(self) -> str:
        return 'triggerSmartContractTransactions'

    def where(self, req: TriggerSmartContractTxRequest) -> Iterable[Expression | None]:
        yield field_in('type', ['TriggerSmartContract'])
        yield field_in('_trigger_smart_contract_sighash', req.get('sighash'))
        yield field_in('_trigger_smart_contract_contract', req.get('contract'))
        yield field_in('_trigger_smart_contract_owner', req.get('owner'))


class _TxItem(Item):
    def table(self) -> Table:
        return _tx_table

    def name(self) -> str:
        return 'transactions'

    def get_selected_fields(self, fields: FieldSelection) -> list[str]:
        return get_selected_fields(fields.get('transaction'), ['transactionIndex'])

    def project(self, fields: FieldSelection) -> str:
        return json_project(self.get_selected_fields(fields), rewrite={
            'timestamp': 'timestamp::text',
            'expiration': 'epoch_ms(expiration)',
            'parameter': 'parameter::json',
            'feeLimit': 'fee_limit::text',
            'fee': 'fee::text',
            'withdrawAmount': 'withdraw_amount::text',
            'unfreezeAmount': 'unfreeze_amount::text',
            'withdrawExpireAmount': 'withdraw_expire_amount::text',
            'energyFee': 'energy_fee::text',
            'energyUsage': 'energy_usage::text',
            'energyUsageTotal': 'energy_usage_total::text',
            'netUsage': 'net_usage::text',
            'netFee': 'net_fee::text',
            'originEnergyUsage': 'origin_energy_usage::text',
            'energyPenaltyTotal': 'energy_penalty_total::text',
            'ret': 'ret::json',
            'cancelUnfreezeV2Amount': 'cancel_unfreeze_v2_amount::json'
        })


class _LogScan(Scan):
    def table(self) -> Table:
        return _logs_table

    def request_name(self) -> str:
        return 'logs'

    def where(self, req: LogRequest) -> Iterable[Expression | None]:
        yield field_in('address', req.get('address'))
        yield field_in('topic0', req.get('topic0'))
        yield field_in('topic1', req.get('topic1'))
        yield field_in('topic2', req.get('topic2'))
        yield field_in('topic3', req.get('topic3'))


class _LogItem(Item):
    def table(self) -> Table:
        return _logs_table

    def name(self) -> str:
        return 'logs'

    def get_selected_fields(self, fields: FieldSelection) -> list[str]:
        return get_selected_fields(fields.get('log'), ['transactionIndex', 'logIndex'])

    def selected_columns(self, fields: FieldSelection) -> list[str]:
        columns = []
        for name in self.get_selected_fields(fields):
            if name == 'topics':
                columns.append('topic0')
                columns.append('topic1')
                columns.append('topic2')
                columns.append('topic3')
            else:
                columns.append(to_snake_case(name))
        return columns

    def project(self, fields: FieldSelection) -> str:
        def rewrite_topics(f: str):
            if f == 'topics':
                return 'topics', f'[topic for topic in list_value(topic0, topic1, topic2, topic3) ' \
                                 f'if topic is not null]'
            else:
                return f

        return json_project(
            map(rewrite_topics, self.get_selected_fields(fields))
        )


class _InternalTxScan(Scan):
    def table(self) -> Table:
        return _internal_tx_table

    def request_name(self) -> str:
        return 'internalTransactions'

    def where(self, req: InternalTxRequest) -> Iterable[Expression | None]:
        yield field_in('caller_address', req.get('caller'))
        yield field_in('transer_to_address', req.get('transferTo'))


class _InternalTxItem(Item):
    def table(self) -> Table:
        return _internal_tx_table

    def name(self) -> str:
        return 'internalTransactions'

    def get_selected_fields(self, fields: FieldSelection) -> list[str]:
        return get_selected_fields(fields.get('internalTransaction'), ['transactionIndex', 'internalTransactionIndex'])

    def project(self, fields: FieldSelection) -> list[str]:
        return json_project(self.get_selected_fields(fields), rewrite={
            'callValueInfo': 'call_value_info::json',
            'transferToAddress': 'transer_to_address::text'
        })


def _build_model():
    tx_scan = _TxScan()
    transfer_tx_scan = _TransferTxScan()
    transfer_asset_tx_scan = _TransferAssetTxScan()
    trigger_contract_tx_scan = _TriggerSmartContractTransferTxScan()
    log_scan = _LogScan()
    internal_tx_scan = _InternalTxScan()

    block_item = _BlockItem()
    tx_item = _TxItem()
    log_item = _LogItem()
    internal_tx_item = _InternalTxItem()

    internal_tx_item.sources.extend([
        internal_tx_scan,
        JoinRel(
            scan=tx_scan,
            include_flag_name='internalTransactions',
            query='SELECT * FROM internal_transactions i, s WHERE '
                  'i.block_number = s.block_number AND '
                  'i.transaction_index = s.transaction_index'
        ),
        JoinRel(
            scan=transfer_tx_scan,
            include_flag_name='internalTransactions',
            query='SELECT * FROM internal_transactions i, s WHERE '
                  'i.block_number = s.block_number AND '
                  'i.transaction_index = s.transaction_index'
        ),
        JoinRel(
            scan=transfer_asset_tx_scan,
            include_flag_name='internalTransactions',
            query='SELECT * FROM internal_transactions i, s WHERE '
                  'i.block_number = s.block_number AND '
                  'i.transaction_index = s.transaction_index'
        ),
        JoinRel(
            scan=trigger_contract_tx_scan,
            include_flag_name='internalTransactions',
            query='SELECT * FROM internal_transactions i, s WHERE '
                  'i.block_number = s.block_number AND '
                  'i.transaction_index = s.transaction_index'
        )
    ])

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
            scan=transfer_tx_scan,
            include_flag_name='logs',
            query='SELECT * FROM logs i, s WHERE '
                  'i.block_number = s.block_number AND '
                  'i.transaction_index = s.transaction_index'
        ),
        JoinRel(
            scan=transfer_asset_tx_scan,
            include_flag_name='logs',
            query='SELECT * FROM logs i, s WHERE '
                  'i.block_number = s.block_number AND '
                  'i.transaction_index = s.transaction_index'
        ),
        JoinRel(
            scan=trigger_contract_tx_scan,
            include_flag_name='logs',
            query='SELECT * FROM logs i, s WHERE '
                  'i.block_number = s.block_number AND '
                  'i.transaction_index = s.transaction_index'
        )
    ])

    tx_item.sources.extend([
        tx_scan,
        transfer_tx_scan,
        transfer_asset_tx_scan,
        trigger_contract_tx_scan,
        RefRel(
            scan=log_scan,
            include_flag_name='transaction',
            scan_columns=['transaction_index']
        ),
        RefRel(
            scan=internal_tx_scan,
            include_flag_name='transaction',
            scan_columns=['transaction_index']
        ),
    ])

    return [
        tx_scan,
        transfer_tx_scan,
        transfer_asset_tx_scan,
        trigger_contract_tx_scan,
        log_scan,
        internal_tx_scan,
        block_item,
        tx_item,
        log_item,
        internal_tx_item,
    ]


MODEL = _build_model()
