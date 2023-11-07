from typing import TypedDict, Iterable

import marshmallow as mm
from pyarrow.dataset import Expression

from sqa.query.model import JoinRel, RefRel, Table, Item, FieldSelection, Scan
from sqa.query.schema import BaseQuerySchema
from sqa.query.util import to_snake_case, json_project, get_selected_fields, field_in


class BlockFieldSelection(TypedDict, total=False):
    number: bool
    hash: bool
    parentHash: bool
    timestamp: bool
    txTrieRoot: bool
    version: bool
    timestamp: bool
    witness_address: bool
    witness_signature: bool


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
    transactionHash: bool
    address: bool
    data: bool
    topics: bool


class InternalTransactionFieldSelection(TypedDict, total=False):
    transactionHash: bool
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


TxRequest = TypedDict('TxRequest', {
    'from': list[str],
    'to': list[str],
    'sighash': list[str],
    'logs': bool,
    'internalTransactions': bool,
}, total=False)


class TraceRequest(TypedDict, total=False):
    caller: list[str]
    transferTo: list[str]
    transaction: bool


def _field_map_schema(typed_dict):
    return mm.fields.Dict(
        mm.fields.Str(validate=lambda k: k in typed_dict.__optional_keys__),
        mm.fields.Boolean(),
        required=False
    )


class _FieldSelectionSchema(mm.Schema):
    block = _field_map_schema(BlockFieldSelection)
    log = _field_map_schema(LogFieldSelection)
    transaction = _field_map_schema(TxFieldSelection)
    internalTransaction = _field_map_schema(InternalTransactionFieldSelection)


class _LogRequestSchema(mm.Schema):
    address = mm.fields.List(mm.fields.Str())
    topic0 = mm.fields.List(mm.fields.Str())
    topic1 = mm.fields.List(mm.fields.Str())
    topic2 = mm.fields.List(mm.fields.Str())
    topic3 = mm.fields.List(mm.fields.Str())
    transaction = mm.fields.Boolean()


_TxRequestSchema = mm.Schema.from_dict({
    'from': mm.fields.List(mm.fields.Str()),
    'to': mm.fields.List(mm.fields.Str()),
    'sighash': mm.fields.List(mm.fields.Str()),
    'logs': mm.fields.Boolean(),
    'internalTransactions': mm.fields.Boolean(),
})


class _InternalTransactionRequestSchema(mm.Schema):
    caller = mm.fields.List(mm.fields.Str())
    transferTo = mm.fields.List(mm.fields.Str())
    transaction = mm.fields.Boolean()


class _QuerySchema(BaseQuerySchema):
    fields = mm.fields.Nested(_FieldSelectionSchema())

    logs = mm.fields.List(mm.fields.Nested(_LogRequestSchema()))

    transactions = mm.fields.List(mm.fields.Nested(_TxRequestSchema()))

    internalTransactions = mm.fields.List(mm.fields.Nested(_InternalTransactionRequestSchema()))


QUERY_SCHEMA = _QuerySchema()


_blocks_table = Table(
    name='blocks',
    primary_key=[]
)


_tx_table = Table(
    name='transactions',
    primary_key=['hash'],
    column_weights={
        'raw_data_hex': 'raw_data_hex_size'
    }
)


_logs_table = Table(
    name='logs',
    primary_key=[],
    column_weights={
        'data': 'data_size'
    }
)


_internal_tx_table = Table(
    name='internal_transactions',
    primary_key=['hash']
)


class _BlockItem(Item):
    def table(self) -> Table:
        return _blocks_table

    def name(self) -> str:
        return 'blocks'

    def get_selected_fields(self, fields: FieldSelection) -> list[str]:
        return get_selected_fields(fields.get('block'), ['number', 'hash', 'parentHash'])

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
        yield field_in('to', req.get('to'))
        yield field_in('from', req.get('from'))
        yield field_in('sighash', req.get('sighash'))


class _TxItem(Item):
    def table(self) -> Table:
        return _tx_table

    def name(self) -> str:
        return 'transactions'

    def get_selected_fields(self, fields: FieldSelection) -> list[str]:
        return get_selected_fields(fields.get('transaction'), [])


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
        return get_selected_fields(fields.get('log'), ['transactionHash'])

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

    def where(self, req: TraceRequest) -> Iterable[Expression | None]:
        yield field_in('caller_address', req.get('caller'))
        yield field_in('transer_to_address', req.get('transferTo'))


class _InternalTxItem(Item):
    def table(self) -> Table:
        return _internal_tx_table

    def name(self) -> str:
        return 'internalTransactions'

    def get_selected_fields(self, fields: FieldSelection) -> list[str]:
        return get_selected_fields(fields.get('internalTransactions'), [])


def _build_model():
    tx_scan = _TxScan()
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
                  'i.transaction_hash = s.hash'
        )
    ])

    log_item.sources.extend([
        log_scan,
        JoinRel(
            scan=tx_scan,
            include_flag_name='logs',
            query='SELECT * FROM logs i, s WHERE '
                  'i.transaction_hash = s.hash'
        )
    ])

    tx_item.sources.extend([
        tx_scan,
        RefRel(
            scan=log_scan,
            include_flag_name='transaction',
            scan_columns=['transaction_hash']
        ),
        RefRel(
            scan=internal_tx_scan,
            include_flag_name='transaction',
            scan_columns=['transaction_hash']
        ),
    ])

    return [
        tx_scan,
        log_scan,
        internal_tx_scan,
        block_item,
        tx_item,
        log_item,
        internal_tx_item,
    ]


MODEL = _build_model()
