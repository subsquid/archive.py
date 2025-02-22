from collections.abc import Iterable
from typing import TypedDict

import marshmallow as mm
from pyarrow.dataset import Expression

from sqa.query.model import Item, JoinRel, Model, RefRel, Scan, Table
from sqa.query.schema import BaseQuerySchema, field_map_schema
from sqa.query.util import field_gte, field_in, field_lte, get_selected_fields, json_project, to_snake_case


class BlockFieldSelection(TypedDict, total=False):
    parentHash: bool
    status: bool
    newRoot: bool
    timestamp: bool
    sequencerAddress: bool
    starknetVersion: bool
    l1GasPriceInFri: bool
    l1GasPriceInWei: bool


class TransactionFieldSelection(TypedDict, total=False):
    transactionHash: bool
    contractAddress: bool
    entryPointSelector: bool
    calldata: bool
    maxFee: bool
    type: bool
    senderAddress: bool
    version: bool
    signature: bool
    nonce: bool
    classHash: bool
    compiledClassHash: bool
    contractAddressSalt: bool
    constructorCalldata: bool
    resourceBounds: bool
    tip: bool
    paymasterData: bool
    accountDeploymentData: bool
    nonceDataAvailabilityMode: bool
    feeDataAvailabilityMode: bool
    messageHash: bool
    actualFee: bool
    finalityStatus: bool

class EventFieldSelection(TypedDict, total=False):
    fromAddress: bool
    keys: bool
    data: bool
    traceAddress: bool


class TraceFieldSelection(TypedDict, total=False):
    traceType: bool
    invocationType: bool
    callerAddress: bool
    contractAddress: bool
    callType: bool
    classHash: bool
    entryPointSelector: bool
    entryPointType: bool
    revertReason: bool
    calldata: bool
    result: bool


class MessageFieldSelection(TypedDict, total=False):
    fromAddress: bool
    toAddress: bool
    payload: bool


class StateUpdateFieldSelection(TypedDict, total=False):
    newRoot: bool
    oldRoot: bool
    deprecatedDeclaredClasses: bool
    declaredClasses: bool
    deployedContracts: bool
    replacedClasses: bool
    nonces: bool

class StorageDiffFieldSelection(TypedDict, total=False):
    value: bool


class FieldSelection(TypedDict, total=False):
    block: BlockFieldSelection
    transaction: TransactionFieldSelection
    event: EventFieldSelection
    trace: TraceFieldSelection
    message: MessageFieldSelection
    stateUpdate: StateUpdateFieldSelection
    storageDiff: StorageDiffFieldSelection


class _FieldSelectionSchema(mm.Schema):
    block = field_map_schema(BlockFieldSelection)
    transaction = field_map_schema(TransactionFieldSelection)
    event = field_map_schema(EventFieldSelection)
    trace = field_map_schema(TraceFieldSelection)
    message = field_map_schema(MessageFieldSelection)
    stateUpdate = field_map_schema(StateUpdateFieldSelection)
    storageDiff = field_map_schema(StorageDiffFieldSelection)


class TransactionRequest(TypedDict, total=False):
    contractAddress: list[str]
    senderAddress: list[str]
    type: list[str]
    firstNonce: int
    lastNonce: int
    events: bool
    traces: bool
    messages: bool


class _TransactionRequestSchema(mm.Schema):
    contractAddress = mm.fields.List(mm.fields.Str())
    senderAddress = mm.fields.List(mm.fields.Str())
    type = mm.fields.List(mm.fields.Str())
    firstNonce = mm.fields.Integer(
        strict=True,
        validate=mm.validate.Range(min=0, min_inclusive=True),
    )
    lastNonce = mm.fields.Integer(
        strict=True,
        validate=mm.validate.Range(min=0, min_inclusive=True),
    )
    events = mm.fields.Boolean()
    traces = mm.fields.Boolean()
    messages = mm.fields.Boolean()


class EventRequest(TypedDict, total=False):
    fromAddress: list[str]
    key0: list[str]
    key1: list[str]
    key2: list[str]
    key3: list[str]
    transaction: bool


class _EventRequestSchema(mm.Schema):
    fromAddress = mm.fields.List(mm.fields.Str())
    key0 = mm.fields.List(mm.fields.Str())
    key1 = mm.fields.List(mm.fields.Str())
    key2 = mm.fields.List(mm.fields.Str())
    key3 = mm.fields.List(mm.fields.Str())
    transaction = mm.fields.Boolean()


class TraceRequest(TypedDict, total=False):
    traceType: list[str]
    invocationType: list[str]
    callerAddress: list[str]
    contractAddress: list[str]
    classHash: list[str]
    callType: list[str]
    entryPointSelector: list[str]
    entryPointType: list[str]
    transaction: bool
    messages: bool
    events: bool


class _TraceRequestSchema(mm.Schema):
    traceType = mm.fields.List(mm.fields.Str())
    invocationType = mm.fields.List(mm.fields.Str())
    callerAddress = mm.fields.List(mm.fields.Str())
    contractAddress = mm.fields.List(mm.fields.Str())
    classHash = mm.fields.List(mm.fields.Str())
    callType = mm.fields.List(mm.fields.Str())
    entryPointSelector = mm.fields.List(mm.fields.Str())
    entryPointType = mm.fields.List(mm.fields.Str())
    transaction = mm.fields.Boolean()
    messages = mm.fields.Boolean()
    events = mm.fields.Boolean()


class MessageRequest(TypedDict, total=False):
    fromAddress: list[str]
    toAddress: list[str]

class _MessageRequestSchema(mm.Schema):
    fromAddress = mm.fields.List(mm.fields.Str())
    toAddress = mm.fields.List(mm.fields.Str())

class StateUpdateRequest(TypedDict, total=False):
    ...


class _StateUpdateRequestSchema(mm.Schema):
    ...


class StorageDiffRequest(TypedDict, total=False):
    address: list[str]
    key: list[str]


class _StorageDiffRequestSchema(mm.Schema):
    address = mm.fields.List(mm.fields.Str())
    key = mm.fields.List(mm.fields.Str())


class _QuerySchema(BaseQuerySchema):
    fields = mm.fields.Nested(_FieldSelectionSchema())
    transactions = mm.fields.List(mm.fields.Nested(_TransactionRequestSchema()))
    events = mm.fields.List(mm.fields.Nested(_EventRequestSchema()))
    traces = mm.fields.List(mm.fields.Nested(_TraceRequestSchema()))
    messages = mm.fields.List(mm.fields.Nested(_MessageRequestSchema()))
    stateUpdates = mm.fields.List(mm.fields.Nested(_StateUpdateRequestSchema()))
    storageDiffs = mm.fields.List(mm.fields.Nested(_StorageDiffRequestSchema()))


QUERY_SCHEMA = _QuerySchema()


_blocks_table = Table(
    name='blocks',
    primary_key=[],
)


_tx_table = Table(
    name='transactions',
    primary_key=['transaction_index'],
    column_weights={},
)


_events_table = Table(
    name='events',
    primary_key=['transaction_index', 'event_index'],
    column_weights={
        'key0': 'keys_size',
        'key1': 0,
        'key2': 0,
        'key3': 0,
        'rest_keys': 0,
    },
)


_traces_table = Table(
    name='traces',
    primary_key=['transaction_index', 'trace_address'],
    column_weights={},
)

_messages_table = Table(
    name='messages',
    primary_key=['transaction_index', 'order'],
    column_weights={},
)


_state_updates_table = Table(
    name='state_updates',
    primary_key=[],
    column_weights={},
)

_storage_diffs_table = Table(
    name='storage_diffs',
    primary_key=['address', 'key'],
    column_weights={},
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
            'timestamp': 'epoch(timestamp)::int64',
        })


class _TxScan(Scan):
    def table(self) -> Table:
        return _tx_table

    def request_name(self) -> str:
        return 'transactions'

    def where(self, req: TransactionRequest) -> Iterable[Expression | None]:
        yield field_in('contract_address', req.get('contractAddress'))
        yield field_in('sender_address', req.get('senderAddress'))
        yield field_in('type', req.get('type'))
        yield field_gte('nonce', req.get('firstNonce'))
        yield field_lte('nonce', req.get('lastNonce'))


class _TxItem(Item):
    def table(self) -> Table:
        return _tx_table

    def name(self) -> str:
        return 'transactions'

    def get_selected_fields(self, fields: FieldSelection) -> list[str]:
        return get_selected_fields(fields.get('transaction'), ['transactionIndex'])

    def selected_columns(self, fields: FieldSelection) -> list[str]:
        columns = []
        for name in self.get_selected_fields(fields):
            if name == 'resourceBounds':
                columns.append('resource_bounds_l1_gas_max_amount')
                columns.append('resource_bounds_l1_gas_max_price_per_unit')
                columns.append('resource_bounds_l2_gas_max_amount')
                columns.append('resource_bounds_l2_gas_max_price_per_unit')
            elif name == 'actualFee':
                columns.append('actual_fee_amount')
                columns.append('actual_fee_unit')
            else:
                columns.append(to_snake_case(name))
        return columns

    def project(self, fields: FieldSelection) -> str:
        # Build a rewrite map for fields that need to become JSON objects
        rewrite = {}

        # If resourceBounds is selected, create a JSON object with the four subfields
        if fields.get('transaction', {}).get('resourceBounds'):
            rewrite['resourceBounds'] = (
                "json_object("
                "'l1GasMaxAmount', resource_bounds_l1_gas_max_amount, "
                "'l1GasMaxPricePerUnit', resource_bounds_l1_gas_max_price_per_unit, "
                "'l2GasMaxAmount', resource_bounds_l2_gas_max_amount, "
                "'l2GasMaxPricePerUnit', resource_bounds_l2_gas_max_price_per_unit)"
            )

        # If actualFee is selected, create a JSON object with amount & unit
        if fields.get('transaction', {}).get('actualFee'):
            rewrite['actualFee'] = (
                "json_object("
                "'amount', actual_fee_amount, "
                "'unit', actual_fee_unit)"
            )

        # Use json_project to build the final JSON with our rewrite rules
        return json_project(self.get_selected_fields(fields), rewrite=rewrite)

class _EventScan(Scan):
    def table(self) -> Table:
        return _events_table

    def request_name(self) -> str:
        return 'events'

    def where(self, req: EventRequest) -> Iterable[Expression | None]:
        yield field_in('from_address', req.get('fromAddress'))
        yield field_in('key0', req.get('key0'))
        yield field_in('key1', req.get('key1'))
        yield field_in('key2', req.get('key2'))
        yield field_in('key3', req.get('key3'))


class _EventItem(Item):
    def table(self) -> Table:
        return _events_table

    def name(self) -> str:
        return 'events'

    def get_selected_fields(self, fields: FieldSelection) -> list[str]:
        return get_selected_fields(fields.get('event'), ['transactionIndex', 'eventIndex'])

    def selected_columns(self, fields: FieldSelection) -> list[str]:
        columns = []
        for name in self.get_selected_fields(fields):
            if name == 'keys':
                columns.append('key0')
                columns.append('key1')
                columns.append('key2')
                columns.append('key3')
                columns.append('rest_keys')
            else:
                columns.append(to_snake_case(name))
        return columns

    def project(self, fields: FieldSelection) -> str:
        return json_project(self.get_selected_fields(fields), rewrite={
            'keys': 'list_concat('
                    '[k for k in list_value(key0, key1, key2, key3) if k is not null], '
                    'rest_keys'
                    ')',
        })


class _TraceScan(Scan):
    def table(self) -> Table:
        return _traces_table

    def request_name(self) -> str:
        return 'traces'

    def where(self, req: TraceRequest) -> Iterable[Expression | None]:
        yield field_in('caller_address', req.get('callerAddress'))
        yield field_in('contract_address', req.get('contractAddress'))
        yield field_in('call_type', req.get('callType'))
        yield field_in('class_hash', req.get('classHash'))
        yield field_in('entry_point_selector', req.get('entryPointSelector'))
        yield field_in('entry_point_type', req.get('entryPointType'))
        yield field_in('trace_type', req.get('traceType'))
        yield field_in('invocation_type', req.get('invocationType'))


class _TraceItem(Item):
    def table(self) -> Table:
        return _traces_table

    def name(self) -> str:
        return 'traces'

    def get_selected_fields(self, fields: FieldSelection) -> list[str]:
        return get_selected_fields(fields.get('trace'), [
            'transactionIndex',
            'traceAddress',
        ])


class _MessageScan(Scan):
    def table(self) -> Table:
        return _messages_table

    def request_name(self) -> str:
        return 'messages'

    def where(self, req: MessageRequest) -> Iterable[Expression | None]:
        yield field_in('from_address', req.get('fromAddress'))
        yield field_in('to_address', req.get('toAddress'))


class _MessageItem(Item):
    def table(self) -> Table:
        return _messages_table

    def name(self) -> str:
        return 'messages'

    def get_selected_fields(self, fields: FieldSelection) -> list[str]:
        return get_selected_fields(fields.get('message'), ['transactionIndex', 'traceAddress', 'order'])


class _StateUpdateScan(Scan):
    def table(self) -> Table:
        return _state_updates_table

    def request_name(self) -> str:
        return 'stateUpdates'

    def where(self, req: StateUpdateRequest) -> Iterable[Expression | None]:  # noqa: ARG002
        return []


class _StateUpdateItem(Item):
    def table(self) -> Table:
        return _state_updates_table

    def name(self) -> str:
        return 'state_updates'

    def get_selected_fields(self, fields: FieldSelection) -> list[str]:
        return get_selected_fields(fields.get('stateUpdate'))


class _StorageDiffScan(Scan):
    def table(self) -> Table:
        return _storage_diffs_table

    def request_name(self) -> str:
        return 'storageDiffs'

    def where(self, req: StorageDiffRequest) -> Iterable[Expression | None]:
        yield field_in('address', req.get('address'))
        yield field_in('key', req.get('key'))


class _StorageDiffItem(Item):
    def table(self) -> Table:
        return _storage_diffs_table

    def name(self) -> str:
        return 'storage_diffs'

    def get_selected_fields(self, fields: FieldSelection) -> list[str]:
        return get_selected_fields(fields.get('storageDiff'), ['address', 'key'])


def _build_model() -> Model:
    tx_scan = _TxScan()
    event_scan = _EventScan()
    trace_scan = _TraceScan()
    message_scan = _MessageScan()
    state_update_scan = _StateUpdateScan()
    storage_diff_scan = _StorageDiffScan()

    block_item = _BlockItem()
    tx_item = _TxItem()
    event_item = _EventItem()
    trace_item = _TraceItem()
    message_item = _MessageItem()
    state_update_item = _StateUpdateItem()
    storage_diff_item = _StorageDiffItem()

    event_item.sources.extend([
        event_scan,
        JoinRel(
            scan=tx_scan,
            include_flag_name='events',
            query='SELECT * FROM events i, s WHERE '
                  'i.block_number = s.block_number AND '
                  'i.transaction_index = s.transaction_index',
        ),
        JoinRel(
            scan=trace_scan,
            include_flag_name='events',
            query='SELECT * FROM events i, s WHERE '
                  'i.block_number = s.block_number AND '
                  'i.transaction_index = s.transaction_index AND '
                  'i.trace_address = s.trace_address',
        ),
    ])

    tx_item.sources.extend([
        tx_scan,
        RefRel(
            scan=event_scan,
            include_flag_name='transaction',
            scan_columns=['transaction_index'],
        ),
        RefRel(
            scan=trace_scan,
            include_flag_name='transaction',
            scan_columns=['transaction_index'],
        ),
    ])

    trace_item.sources.extend([
        trace_scan,
        JoinRel(
            scan=tx_scan,
            include_flag_name='traces',
            query='SELECT * FROM traces i, s WHERE '
                  'i.block_number = s.block_number AND '
                  'i.transaction_index = s.transaction_index',
        ),
    ])

    message_item.sources.extend([
        message_scan,
        JoinRel(
            scan=trace_scan,
            include_flag_name='messages',
            scan_columns=['transaction_index', 'trace_address'],
            query='SELECT * FROM messages i, s WHERE '
                  'i.block_number = s.block_number AND '
                  'i.transaction_index = s.transaction_index AND '
                  'i.trace_address = s.trace_address',
        ),
        JoinRel(
            scan=tx_scan,
            include_flag_name='messages',
            scan_columns=['transaction_index'],
            query='SELECT * FROM messages i, s WHERE '
                  'i.block_number = s.block_number AND '
                  'i.transaction_index = s.transaction_index',
        ),
    ])

    state_update_item.sources.extend([state_update_scan])

    storage_diff_item.sources.extend([storage_diff_scan])

    return [tx_scan, event_scan, trace_scan, message_scan, state_update_scan, storage_diff_scan,
            block_item, tx_item, event_item, trace_item, message_item, state_update_item, storage_diff_item]


MODEL = _build_model()
