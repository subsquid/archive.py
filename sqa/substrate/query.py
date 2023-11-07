from typing import TypedDict

import marshmallow as mm

from sqa.query.model import STable, RTable, Builder, Model, JoinRel, RefRel
from sqa.query.schema import BaseQuerySchema
from sqa.query.util import json_project


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


def _field_map_schema(typed_dict):
    return mm.fields.Dict(
        mm.fields.Str(validate=lambda k: k in typed_dict.__optional_keys__),
        mm.fields.Boolean(),
        required=False
    )


class _FieldSelectionSchema(mm.Schema):
    block = _field_map_schema(BlockFieldSelection)
    extrinsic = _field_map_schema(ExtrinsicFieldSelection)
    call = _field_map_schema(CallFieldSelection)
    event = _field_map_schema(EventFieldSelection)


class _CallRelations(TypedDict, total=False):
    extrinsic: bool
    stack: bool
    events: bool


class _CallRelationsSchema(mm.Schema):
    extrinsic = mm.fields.Boolean()
    stack = mm.fields.Boolean()
    events = mm.fields.Boolean()


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


class _BlocksTable(STable):
    def table_name(self) -> str:
        return 'blocks'

    def field_selection_name(self) -> str:
        return 'block'

    def primary_key(self) -> tuple[str, ...]:
        return 'number',

    def required_fields(self) -> tuple[str, ...]:
        return 'number', 'hash', 'parentHash'

    def field_weights(self) -> dict[str, int]:
        return {
            'digest': 2
        }

    def project(self, fields: dict, prefix=''):
        def rewrite_timestamp(f: str):
            if f == 'timestamp':
                return 'timestamp', f'epoch({prefix}timestamp)'
            else:
                return f

        return json_project(
            map(rewrite_timestamp, self.get_selected_fields(fields)),
            prefix=prefix
        )


class _R_BaseEvent(RTable):
    def table_name(self) -> str:
        return 'events'

    def columns(self) -> tuple[str, ...]:
        return 'index', 'extrinsic_index', 'call_address'


class _REvents(_R_BaseEvent):
    def where(self, builder: Builder, req: EventRequest):
        yield builder.in_condition('name', req.get('name'))


class _REvmLogs(_R_BaseEvent):
    def request_name(self) -> str:
        return 'evmLogs'

    def where(self, builder: Builder, req: EvmLogRequest):
        yield builder.in_condition('name', ['EVM.Log'])
        yield builder.in_condition('_evm_log_address', req.get('address'))
        yield builder.in_condition('_evm_log_topic0', req.get('topic0'))
        yield builder.in_condition('_evm_log_topic1', req.get('topic1'))
        yield builder.in_condition('_evm_log_topic2', req.get('topic2'))
        yield builder.in_condition('_evm_log_topic3', req.get('topic3'))


class _RContractsEvents(_R_BaseEvent):
    def request_name(self) -> str:
        return 'contractsEvents'

    def where(self, builder: Builder, req: ContractsContractEmittedRequest):
        yield builder.in_condition('name', ['Contracts.ContractEmitted'])
        yield builder.in_condition('_contract_address', req.get('contractAddress'))


class _RGearUserMessagesEnqueued(_R_BaseEvent):
    def request_name(self) -> str:
        return 'gearMessagesEnqueued'

    def where(self, builder: Builder, req: GearMessageEnqueuedRequest):
        yield builder.in_condition('name', ['Gear.UserMessageEnqueued'])
        yield builder.in_condition('_gear_program_id', req.get('programId'))


class _RGearUserMessagesSent(_R_BaseEvent):
    def request_name(self) -> str:
        return 'gearUserMessagesSent'

    def where(self, builder: Builder, req: GearUserMessageSentRequest):
        yield builder.in_condition('name', ['Gear.UserMessageSent'])
        yield builder.in_condition('_gear_program_id', req.get('programId'))


class _SEvents(STable):
    def table_name(self) -> str:
        return 'events'

    def field_selection_name(self) -> str:
        return 'event'

    def primary_key(self) -> tuple[str, ...]:
        return 'index',

    def required_fields(self) -> tuple[str, ...]:
        return 'index', 'extrinsicIndex', 'callAddress'

    def field_weights(self) -> dict[str, int]:
        return {
            'args': 4
        }

    def project(self, fields: dict, prefix: str = '') -> str:
        def rewrite(f: str):
            if f == 'args':
                return f, f'{prefix}{f}::json'
            else:
                return f

        return json_project(
            map(rewrite, self.get_selected_fields(fields)),
            prefix=prefix
        )


class _RCalls(RTable):
    def table_name(self) -> str:
        return 'calls'

    def columns(self) -> tuple[str, ...]:
        return 'extrinsic_index', 'address'

    def where(self, builder: Builder, req: CallRequest):
        yield builder.in_condition('name', req.get('name'))


class _REthereumTransactCalls(RTable):
    def table_name(self) -> str:
        return 'calls'

    def request_name(self) -> str:
        return 'ethereumTransactions'

    def columns(self) -> tuple[str, ...]:
        return 'extrinsic_index', 'address'

    def where(self, builder: Builder, req: EthereumTransactRequest):
        yield builder.in_condition('name', ['Ethereum.transact'])
        yield builder.in_condition('_ethereum_transact_to', req.get('to'))
        yield builder.in_condition('_ethereum_transact_sighash', req.get('sighash'))


class _SCalls(STable):
    def table_name(self) -> str:
        return 'calls'

    def field_selection_name(self) -> str:
        return 'call'

    def primary_key(self) -> tuple[str, ...]:
        return 'extrinsicIndex', 'address'

    def field_weights(self) -> dict[str, int]:
        return {
            'args': 4
        }

    def project(self, fields: dict, prefix: str = '') -> str:
        def rewrite(f: str):
            if f in ('args', 'origin', 'error'):
                return f, f'{prefix}{f}::json'
            else:
                return f

        return json_project(
            map(rewrite, self.get_selected_fields(fields)),
            prefix=prefix
        )


class _SExtrinsics(STable):
    def table_name(self) -> str:
        return 'extrinsics'

    def field_selection_name(self) -> str:
        return 'extrinsic'

    def primary_key(self) -> tuple[str, ...]:
        return 'index',

    def field_weights(self) -> dict[str, int]:
        return {
            'signature': 2
        }

    def project(self, fields: dict, prefix: str = '') -> str:
        def rewrite(f: str):
            if f in ('fee', 'tip'):
                return f, f'{prefix}{f}::string'
            elif f in ('signature', 'error'):
                return f, f'{prefix}{f}::json'
            else:
                return f

        return json_project(
            map(rewrite, self.get_selected_fields(fields)),
            prefix=prefix
        )


def _build_model() -> Model:
    r_events = _REvents()
    r_evm_logs = _REvmLogs()
    r_contracts_events = _RContractsEvents()
    r_gear_enqueued = _RGearUserMessagesEnqueued()
    r_gear_sent = _RGearUserMessagesSent()

    r_calls = _RCalls()
    r_ethereum_transact_calls = _REthereumTransactCalls()

    s_events = _SEvents()
    s_calls = _SCalls()
    s_extrinsics = _SExtrinsics()

    s_events.sources.extend([
        r_events,
        r_evm_logs,
        r_contracts_events,
        r_gear_enqueued,
        r_gear_sent
    ])

    for rt in (r_calls, r_ethereum_transact_calls):
        s_events.sources.append(
            JoinRel(
                table=rt,
                include_flag_name='events',
                join_condition='s.extrinsic_index = r.extrinsic_index AND '
                               'len(s.call_address) >= len(r.address) AND '
                               's.call_address[1:len(r.address)] = r.address'
            )
        )

    s_calls.sources.extend([
        r_calls,
        r_ethereum_transact_calls,
        JoinRel(
            table=r_calls,
            include_flag_name='subcalls',
            join_condition='s.extrinsic_index = r.extrinsic_index AND '
                           'len(s.address) > len(r.address) AND '
                           's.address[1:len(r.address)] = r.address'
        )
    ])

    for rt in (r_calls, r_ethereum_transact_calls):
        s_calls.sources.append(
            JoinRel(
                table=rt,
                include_flag_name='stack',
                join_condition='s.extrinsic_index = r.extrinsic_index AND '
                               'len(s.address) < len(r.address) AND '
                               's.address = r.address[1:len(s.address)]'
            ),
        )

    for r_ev in (r_events, r_evm_logs, r_contracts_events, r_gear_enqueued, r_gear_sent):
        s_calls.sources.extend([
            RefRel(
                table=r_ev,
                include_flag_name='call',
                key=['extrinsic_index', 'call_address']
            ),
            JoinRel(
                table=r_ev,
                include_flag_name='stack',
                join_condition='s.extrinsic_index = r.extrinsic_index AND '
                               'r.call_address is not null AND'
                               'len(s.address) <= len(r.call_address) AND '
                               's.address = r.call_address[1:len(s.address)]'
            )
        ])

        s_extrinsics.sources.extend([
            RefRel(
                table=r_ev,
                include_flag_name='extrinsic',
                key=['extrinsic_index']
            ),
        ])

    s_extrinsics.sources.extend([
        RefRel(
            table=r_calls,
            include_flag_name='extrinsic',
            key=['extrinsic_index']
        )
    ])

    return [
        _BlocksTable(),
        r_events,
        r_evm_logs,
        r_contracts_events,
        r_gear_sent,
        r_gear_enqueued,
        r_calls,
        r_ethereum_transact_calls,
        s_events,
        s_calls,
        s_extrinsics
    ]


MODEL = _build_model()
