from typing import Iterable, TypedDict

import marshmallow as mm
from pyarrow.dataset import Expression

from sqa.query.model import Item, JoinRel, Model, RefRel, Scan, Table
from sqa.query.schema import BaseQuerySchema
from sqa.query.schema import field_map_schema
from sqa.query.util import field_gte, field_in, field_lte, get_selected_fields, json_project, to_snake_case


class BlockFieldSelection(TypedDict, total=False):
    parentHash: bool
    status: bool
    newRoot: bool
    timestamp: bool
    sequencerAddress: bool


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


class EventFieldSelection(TypedDict, total=False):
    fromAddress: bool
    keys: bool
    data: bool


class TraceFieldSelection(TypedDict, total=False):
    invocationType: bool
    callerAddress: bool
    callContractAddress: bool
    callType: bool
    callClassHash: bool
    callEntryPointSelector: bool
    callEntryPointType: bool
    callRevertReason: bool
    calldata: bool
    callResult: bool
    parentIndex: bool
    traceType: bool


class StateUpdateFieldSelection(TypedDict, total=False):
    blockNumber: bool
    blockHash: bool
    newRoot: bool
    oldRoot: bool
    storageDiffsAddress: bool
    storageDiffsKeys: bool
    storageDiffsValues: bool
    deprecatedDeclaredClasses: bool
    declaredClassesClassHash: bool
    declaredClassesCompiledClassHash: bool
    deployedContractsAddress: bool
    deployedContractsClassHash: bool
    replacedClassesContractAddress: bool
    replacedClassesClassHash: bool
    noncesContractAddress: bool
    noncesNonce: bool

class FieldSelection(TypedDict, total=False):
    block: BlockFieldSelection
    transaction: TransactionFieldSelection
    event: EventFieldSelection
    trace: TraceFieldSelection
    stateUpdate: StateUpdateFieldSelection


class _FieldSelectionSchema(mm.Schema):
    block = field_map_schema(BlockFieldSelection)
    transaction = field_map_schema(TransactionFieldSelection)
    event = field_map_schema(EventFieldSelection)
    trace = field_map_schema(TraceFieldSelection)
    stateUpdate = field_map_schema(StateUpdateFieldSelection)


class TransactionRequest(TypedDict, total=False):
    contractAddress: list[str]
    senderAddress: list[str]
    type: list[str]
    firstNonce: int
    lastNonce: int
    events: bool


class _TransactionRequestSchema(mm.Schema):
    contractAddress = mm.fields.List(mm.fields.Str())
    senderAddress = mm.fields.List(mm.fields.Str())
    type = mm.fields.List(mm.fields.Str())
    firstNonce = mm.fields.Integer(
        strict=True,
        validate=mm.validate.Range(min=0, min_inclusive=True)
    ),
    lastNonce = mm.fields.Integer(
        strict=True,
        validate=mm.validate.Range(min=0, min_inclusive=True)
    )
    events = mm.fields.Boolean()


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
    # TODO: potentialy filter by calldata simular to keys of events
    invocationType: list[str]
    callerAddress: list[str]
    callContractAddress: list[str]
    callClassHash: list[str]
    callType: list[str]
    callEntryPointSelector: list[str]
    callEntryPointType: list[str]
    traceType: list[str]
    transaction: bool


class _TraceRequestSchema(mm.Schema):
    invocationType: list[str]
    callerAddress = mm.fields.List(mm.fields.Str())
    callContractAddress = mm.fields.List(mm.fields.Str())
    callClassHash = mm.fields.List(mm.fields.Str())
    callType = mm.fields.List(mm.fields.Str())
    callEntryPointSelector = mm.fields.List(mm.fields.Str())
    callEntryPointType = mm.fields.List(mm.fields.Str())
    traceType = mm.fields.List(mm.fields.Str())
    transaction = mm.fields.Boolean()


class StateUpdateRequest(TypedDict, total=False):
    newRoot: list[str]
    oldRoot: list[str]


class _StateUpdateRequestSchema(mm.Schema):
    newRoot = mm.fields.List(mm.fields.Str())
    oldRoot = mm.fields.List(mm.fields.Str())


class _QuerySchema(BaseQuerySchema):
    fields = mm.fields.Nested(_FieldSelectionSchema())
    transactions = mm.fields.List(mm.fields.Nested(_TransactionRequestSchema()))
    events = mm.fields.List(mm.fields.Nested(_EventRequestSchema()))
    traces = mm.fields.List(mm.fields.Nested(_TraceRequestSchema()))
    stateUpdates = mm.fields.List(mm.fields.Nested(_StateUpdateRequestSchema()))


QUERY_SCHEMA = _QuerySchema()


_blocks_table = Table(
    name='blocks',
    primary_key=[],
)


_tx_table = Table(
    name='transactions',
    primary_key=['transaction_index'],
    column_weights={
        'calldata': 'calldata_size',
        'signature': 'signature_size',
        'constructor_calldata':  'constructor_calldata_size'
    }
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
        'data': 'data_size'
    }
)


_traces_table = Table(
    name='traces',
    primary_key=['transaction_index', 'call_index'],
    column_weights={
        'calldata': 'calldata_size',
        'call_result': 'call_result_size',
        'call_events_keys': 'call_events_keys_size',
        'call_events_data': 'call_events_data_size',
        'call_events_order': 'call_events_order_size',
        'call_messages_payload': 'call_messages_payload_size',
        'call_messages_from_address': 'call_messages_from_address_size',
        'call_messages_to_address': 'call_messages_to_address_size',
        'call_messages_order': 'call_messages_order_size',
    }
)


_state_updates_table = Table(
    name='state_updates',
    primary_key=[],
    column_weights={
        'storage_diffs_address': 'storage_diffs_address_size',
        'storage_diffs_keys': 'storage_diffs_keys_size',
        'storage_diffs_values': 'storage_diffs_values_size',
        'deprecated_declared_classes': 'deprecated_declared_classes_size',
        'declared_classes_class_hash': 'declared_classes_class_hash_size',
        'declared_classes_compiled_class_hash': 'declared_classes_compiled_class_hash_size',
        'deployed_contracts_address': 'deployed_contracts_address_size',
        'deployed_contracts_class_hash': 'deployed_contracts_class_hash_size',
        'replaced_classes_contract_address': 'replaced_classes_contract_address_size',
        'replaced_classes_class_hash': 'replaced_classes_class_hash_size',
        'nonces_contract_address': 'nonces_contract_address_size',
        'nonces_nonce': 'nonces_nonce_size',
    }
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
            'timestamp': 'epoch(timestamp)::int64'
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
            'keys': f'list_concat('
                    f'[k for k in list_value(key0, key1, key2, key3) if k is not null], '
                    f'rest_keys'
                    f')'
        })


class _TraceScan(Scan):
    def table(self) -> Table:
        return _traces_table

    def request_name(self) -> str:
        return 'traces'

    def where(self, req: TraceRequest) -> Iterable[Expression | None]:
        yield field_in('caller_address', req.get('callerAddress'))
        yield field_in('call_contract_address', req.get('callContractAddress'))
        yield field_in('call_type', req.get('callType'))
        yield field_in('call_class_hash', req.get('callClassHash'))
        yield field_in('call_entry_point_selector', req.get('callEntryPointSelector'))
        yield field_in('call_entry_point_type', req.get('callEntryPointType'))
        yield field_in('trace_type', req.get('traceType'))
        yield field_in('invocation_type', req.get('invocationType'))


class _TraceItem(Item):
    def table(self) -> Table:
        return _traces_table

    def name(self) -> str:
        return 'traces'

    def get_selected_fields(self, fields: FieldSelection) -> list[str]:
        return get_selected_fields(fields.get('trace'), [
            'transaction_index',
            'call_index'
        ])


class _StateUpdateScan(Scan):
    def table(self) -> Table:
        return _state_updates_table

    def request_name(self) -> str:
        return 'stateUpdates'

    def where(self, req: StateUpdateRequest) -> Iterable[Expression | None]:
        yield field_in('new_root', req.get('newRoot'))
        yield field_in('old_root', req.get('oldRoot'))


class _StateUpdateItem(Item):
    def table(self) -> Table:
        return _state_updates_table

    def name(self) -> str:
        return 'state_updates'

    def get_selected_fields(self, fields: FieldSelection) -> list[str]:
        return get_selected_fields(fields.get('stateUpdate'))


def _build_model() -> Model:
    tx_scan = _TxScan()
    event_scan = _EventScan()
    trace_scan = _TraceScan()
    state_update_scan = _StateUpdateScan()

    block_item = _BlockItem()
    tx_item = _TxItem()
    event_item = _EventItem()
    trace_item = _TraceItem()
    state_update_item = _StateUpdateItem()

    event_item.sources.extend([
        event_scan,
        JoinRel(
            scan=tx_scan,
            include_flag_name='events',
            query='SELECT * FROM events i, s WHERE '
                  'i.block_number = s.block_number AND '
                  'i.transaction_index = s.transaction_index'
        )
    ])

    tx_item.sources.extend([
        tx_scan,
        RefRel(
            scan=event_scan,
            include_flag_name='transaction',
            scan_columns=['transaction_index']
        ),
        RefRel(
            scan=trace_scan,
            include_flag_name='transaction',
            scan_columns=['transaction_index']
        )
    ])

    trace_item.sources.extend([
        trace_scan,
        JoinRel(
            scan=tx_scan,
            include_flag_name='traces',
            query='SELECT * FROM traces i, s WHERE '
                  'i.block_number = s.block_number AND '
                  'i.transaction_index = s.transaction_index'
        )
    ])

    state_update_item.sources.extend([state_update_scan])

    return [tx_scan, event_scan, trace_scan, state_update_scan, block_item, tx_item, event_item, trace_item, state_update_item]


MODEL = _build_model()
