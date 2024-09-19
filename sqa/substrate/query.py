from typing import TypedDict, Iterable

import marshmallow as mm
import pyarrow.dataset

from sqa.query.model import Model, JoinRel, RefRel, Table, Item, FieldSelection, Scan, SubRel
from sqa.query.schema import BaseQuerySchema, field_map_schema
from sqa.query.util import json_project, get_selected_fields, field_in


class BlockFieldSelection(TypedDict, total=False):
    hash: bool
    parentHash: bool
    stateRoot: bool
    extrinsicsRoot: bool
    digest: bool
    specName: bool
    specVersion: bool
    implName: bool
    implVersion: bool
    timestamp: bool
    validator: bool


class ExtrinsicFieldSelection(TypedDict, total=False):
    version: bool
    signature: bool
    fee: bool
    tip: bool
    error: bool
    success: bool
    hash: bool


class CallFieldSelection(TypedDict, total=False):
    name: bool
    args: bool
    origin: bool
    error: bool
    success: bool


class EventFieldSelection(TypedDict, total=False):
    name: bool
    args: bool
    phase: bool
    extrinsicIndex: bool
    callAddress: bool
    topics: bool


class _FieldSelectionSchema(mm.Schema):
    block = field_map_schema(BlockFieldSelection)
    extrinsic = field_map_schema(ExtrinsicFieldSelection)
    call = field_map_schema(CallFieldSelection)
    event = field_map_schema(EventFieldSelection)


class _CallRelations(TypedDict, total=False):
    extrinsic: bool
    stack: bool
    events: bool
    siblings: bool


class _CallRelationsSchema(mm.Schema):
    extrinsic = mm.fields.Boolean()
    stack = mm.fields.Boolean()
    events = mm.fields.Boolean()
    siblings = mm.fields.Boolean()


class CallRequest(_CallRelations):
    name: list[str]
    subcalls: bool


class _CallRequestSchema(_CallRelationsSchema):
    name = mm.fields.List(mm.fields.Str())
    subcalls = mm.fields.Boolean()


class _EventRelations(TypedDict, total=False):
    extrinsic: bool
    call: bool
    stack: bool


class _EventRelationsSchema(mm.Schema):
    extrinsic = mm.fields.Boolean()
    call = mm.fields.Boolean()
    stack = mm.fields.Boolean()


class EventRequest(_EventRelations):
    name: list[str]


class _EventRequestSchema(_EventRelationsSchema):
    name = mm.fields.List(mm.fields.Str())


class EvmLogRequest(_EventRelations):
    address: list[str]
    topic0: list[str]
    topic1: list[str]
    topic2: list[str]
    topic3: list[str]


class _EvmLogRequestSchema(_EventRelationsSchema):
    address = mm.fields.List(mm.fields.Str())
    topic0 = mm.fields.List(mm.fields.Str())
    topic1 = mm.fields.List(mm.fields.Str())
    topic2 = mm.fields.List(mm.fields.Str())
    topic3 = mm.fields.List(mm.fields.Str())


class ContractsContractEmittedRequest(_EventRelations):
    contractAddress: list[str]


class _ContractsContractEmittedRequestSchema(_EventRelationsSchema):
    contractAddress = mm.fields.List(mm.fields.Str())


class GearMessageEnqueuedRequest(_EventRelations):
    programId: list[str]


class _GearMessageEnqueuedRequestSchema(_EventRelationsSchema):
    programId = mm.fields.List(mm.fields.Str())


class GearUserMessageSentRequest(_EventRelations):
    programId: list[str]


class _GearUserMessageSentRequestSchema(_EventRelationsSchema):
    programId = mm.fields.List(mm.fields.Str())


class EthereumTransactRequest(_CallRelations):
    to: list[str]
    sighash: list[str]


class _EthereumTransactRequestSchema(_CallRelationsSchema):
    to = mm.fields.List(mm.fields.Str())
    sighash = mm.fields.List(mm.fields.Str())


class _QuerySchema(BaseQuerySchema):
    fields = mm.fields.Nested(_FieldSelectionSchema())
    events = mm.fields.List(mm.fields.Nested(_EventRequestSchema()))
    calls = mm.fields.List(mm.fields.Nested(_CallRequestSchema()))
    evmLogs = mm.fields.List(mm.fields.Nested(_EvmLogRequestSchema()))
    ethereumTransactions = mm.fields.List(mm.fields.Nested(_EthereumTransactRequestSchema()))
    contractsEvents = mm.fields.List(mm.fields.Nested(_ContractsContractEmittedRequestSchema()))
    gearMessagesEnqueued = mm.fields.List(mm.fields.Nested(_GearMessageEnqueuedRequestSchema()))
    gearUserMessagesSent = mm.fields.List(mm.fields.Nested(_GearUserMessageSentRequestSchema()))


QUERY_SCHEMA = _QuerySchema()


_blocks_table = Table(
    name='blocks',
    primary_key=[],
    column_weights={
        'digest': 32 * 4
    }
)


_events_table = Table(
    name='events',
    primary_key=['index'],
    column_weights={
        'args': 'args_size'
    }
)


_calls_table = Table(
    name='calls',
    primary_key=['extrinsic_index', 'address'],
    column_weights={
        'args': 'args_size'
    }
)


_extrinsics_table = Table(
    name='extrinsics',
    primary_key=['index'],
    column_weights={
        'signature': 4 * 32
    }
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


class _EventScan(Scan):
    def table(self) -> Table:
        return _events_table

    def request_name(self) -> str:
        return 'events'

    def where(self, req: EventRequest) -> Iterable[pyarrow.dataset.Expression | None]:
        yield field_in('name', req.get('name'))


class _EvmLogScan(Scan):
    def table(self) -> Table:
        return _events_table

    def request_name(self) -> str:
        return 'evmLogs'

    def where(self, req: EvmLogRequest) -> Iterable[pyarrow.dataset.Expression | None]:
        yield field_in('name', ['EVM.Log'])
        yield field_in('_evm_log_address', req.get('address'))
        yield field_in('_evm_log_topic0', req.get('topic0'))
        yield field_in('_evm_log_topic1', req.get('topic1'))
        yield field_in('_evm_log_topic2', req.get('topic2'))
        yield field_in('_evm_log_topic3', req.get('topic3'))


class _ContractEventScan(Scan):
    def table(self) -> Table:
        return _events_table

    def request_name(self) -> str:
        return 'contractsEvents'

    def where(self, req: ContractsContractEmittedRequest) -> Iterable[pyarrow.dataset.Expression | None]:
        yield field_in('name', ['Contracts.ContractEmitted'])
        yield field_in('_contract_address', req.get('contractAddress'))


class _GearUserMessageEnqueuedScan(Scan):
    def table(self) -> Table:
        return _events_table

    def request_name(self) -> str:
        return 'gearMessagesEnqueued'

    def where(self, req: GearMessageEnqueuedRequest) -> Iterable[pyarrow.dataset.Expression | None]:
        yield field_in('name', ['Gear.UserMessageEnqueued'])
        yield field_in('_gear_program_id', req.get('programId'))


class _GearUserMessageSentScan(Scan):
    def table(self) -> Table:
        return _events_table

    def request_name(self) -> str:
        return 'gearUserMessagesSent'

    def where(self, req: GearUserMessageSentRequest) -> Iterable[pyarrow.dataset.Expression | None]:
        yield field_in('name', ['Gear.UserMessageSent'])
        yield field_in('_gear_program_id', req.get('programId'))


class _EventItem(Item):
    def table(self) -> Table:
        return _events_table

    def name(self) -> str:
        return 'events'

    def get_selected_fields(self, fields: FieldSelection) -> list[str]:
        return get_selected_fields(fields.get('event'), ['index', 'extrinsicIndex', 'callAddress'])

    def project(self, fields: FieldSelection) -> str:
        def rewrite(f: str):
            if f == 'args':
                return f, f'{f}::json'
            else:
                return f

        return json_project(
            map(rewrite, self.get_selected_fields(fields)),
        )


class _CallScan(Scan):
    def table(self) -> Table:
        return _calls_table

    def request_name(self) -> str:
        return 'calls'

    def where(self, req: CallRequest) -> Iterable[pyarrow.dataset.Expression | None]:
        yield field_in('name', req.get('name'))


class _EthereumTransactCallScan(Scan):
    def table(self) -> Table:
        return _calls_table

    def request_name(self) -> str:
        return 'ethereumTransactions'

    def where(self, req: EthereumTransactRequest) -> Iterable[pyarrow.dataset.Expression | None]:
        yield field_in('name', ['Ethereum.transact'])
        yield field_in('_ethereum_transact_to', req.get('to'))
        yield field_in('_ethereum_transact_sighash', req.get('sighash'))


class _CallItem(Item):
    def table(self) -> Table:
        return _calls_table

    def name(self) -> str:
        return 'calls'

    def get_selected_fields(self, fields: FieldSelection) -> list[str]:
        return get_selected_fields(fields.get('call'), ['extrinsicIndex', 'address'])

    def project(self, fields: FieldSelection) -> str:
        def rewrite(f: str):
            if f in ('args', 'origin', 'error'):
                return f, f'{f}::json'
            else:
                return f

        return json_project(
            map(rewrite, self.get_selected_fields(fields))
        )


class _ExtrinsicItem(Item):
    def table(self) -> Table:
        return _extrinsics_table

    def name(self) -> str:
        return 'extrinsics'

    def get_selected_fields(self, fields: FieldSelection) -> list[str]:
        return get_selected_fields(fields.get('extrinsic'), ['index'])

    def project(self, fields: FieldSelection) -> str:
        def rewrite(f: str):
            if f in ('fee', 'tip'):
                return f, f'{f}::string'
            elif f in ('signature', 'error'):
                return f, f'{f}::json'
            else:
                return f

        return json_project(
            map(rewrite, self.get_selected_fields(fields))
        )


def _build_model() -> Model:
    event_scan = _EventScan()
    evm_log_scan = _EvmLogScan()
    gear_message_enqueued_scan = _GearUserMessageEnqueuedScan()
    gear_message_sent_scan = _GearUserMessageSentScan()
    contract_event_scan = _ContractEventScan()

    call_scan = _CallScan()
    ethereum_transact_scan = _EthereumTransactCallScan()

    block_item = _BlockItem()
    event_item = _EventItem()
    call_item = _CallItem()
    extrinsic_item = _ExtrinsicItem()

    event_item.sources.extend([
        event_scan,
        evm_log_scan,
        contract_event_scan,
        gear_message_enqueued_scan,
        gear_message_sent_scan
    ])

    for s in (call_scan, ethereum_transact_scan):
        event_item.sources.append(
            JoinRel(
                scan=s,
                include_flag_name='events',
                query='SELECT * FROM events i, s WHERE '
                      'i.block_number = s.block_number AND '
                      'i.extrinsic_index = s.extrinsic_index AND '
                      'len(i.call_address) >= len(s.address) AND '
                      'i.call_address[1:len(s.address)] = s.address'
            )
        )

    call_item.sources.extend([
        call_scan,
        ethereum_transact_scan,
        JoinRel(
            scan=call_scan,
            include_flag_name='subcalls',
            query='SELECT * FROM calls i, s WHERE '
                  'i.block_number = s.block_number AND '
                  'i.extrinsic_index = s.extrinsic_index AND '
                  'len(i.address) > len(s.address) AND '
                  'i.address[1:len(s.address)] = s.address'
        )
    ])

    for s in (call_scan, ethereum_transact_scan):
        call_item.sources.extend([
            JoinRel(
                scan=s,
                include_flag_name='stack',
                query='SELECT * FROM calls i, s WHERE '
                      'i.block_number = s.block_number AND '
                      'i.extrinsic_index = s.extrinsic_index AND '
                      'len(i.address) < len(s.address) AND '
                      'i.address = s.address[1:len(i.address)]'
            ),
            JoinRel(
                scan=s,
                include_flag_name='siblings',
                query='SELECT * FROM calls i, s WHERE '
                    'i.block_number = s.block_number AND '
                    'i.extrinsic_index = s.extrinsic_index AND '
                    'len(i.address) = len(s.address)'
            )
        ])

    for s in (event_scan,
              evm_log_scan,
              contract_event_scan,
              gear_message_enqueued_scan,
              gear_message_sent_scan
              ):
        call_item.sources.extend([
            RefRel(
                scan=s,
                include_flag_name='call',
                scan_columns=['extrinsic_index', 'call_address']
            ),
            JoinRel(
                scan=s,
                include_flag_name='stack',
                scan_columns=['extrinsic_index', 'call_address'],
                query='SELECT * FROM calls i, s WHERE '
                      'i.block_number = s.block_number AND '
                      'i.extrinsic_index = s.extrinsic_index AND '
                      's.call_address is not null AND '
                      'len(i.address) <= len(s.call_address) AND '
                      'i.address = s.call_address[1:len(i.address)]'
            )
        ])

        extrinsic_item.sources.extend([
            RefRel(
                scan=s,
                include_flag_name='extrinsic',
                scan_columns=['extrinsic_index']
            ),
        ])

    extrinsic_item.sources.extend([
        RefRel(
            scan=call_scan,
            include_flag_name='extrinsic',
            scan_columns=['extrinsic_index']
        )
    ])

    return [
        event_scan,
        evm_log_scan,
        gear_message_sent_scan,
        gear_message_enqueued_scan,
        contract_event_scan,
        call_scan,
        ethereum_transact_scan,
        block_item,
        event_item,
        call_item,
        extrinsic_item
    ]


MODEL = _build_model()
