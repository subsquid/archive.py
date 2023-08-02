from typing import TypedDict

import marshmallow as mm

from sqa.query.model import STable, RTable, Builder, Model, JoinRel, RefRel
from sqa.query.schema import BaseQuerySchema


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


class _CallRelations(TypedDict, total=False):
    extrinsic: bool
    stack: bool
    events: bool


class _EventRelations(TypedDict, total=False):
    extrinsic: bool
    call: bool
    stack: bool


class CallRequest(_CallRelations):
    name: list[str]
    subcalls: bool


class EventRequest(_EventRelations):
    name: list[str]


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


class _CallRelationsSchema(mm.Schema):
    extrinsic = mm.fields.Boolean()
    stack = mm.fields.Boolean()
    events = mm.fields.Boolean()


class _CallRequestSchema(_CallRelationsSchema):
    name = mm.fields.List(mm.fields.Str())


class _EventRelationsSchema(mm.Schema):
    extrinsic = mm.fields.Boolean()
    call = mm.fields.Boolean()
    stack = mm.fields.Boolean()


class _EventRequestSchema(_EventRelationsSchema):
    name = mm.fields.List(mm.fields.Str())


class _QuerySchema(BaseQuerySchema):
    fields = mm.fields.Nested(_FieldSelectionSchema())
    calls = mm.fields.List(mm.fields.Nested(_CallRelationsSchema()))
    events = mm.fields.List(mm.fields.Nested(_EventRequestSchema()))


QUERY_SCHEMA = _QuerySchema()


class _BlocksTable(STable):
    def table_name(self) -> str:
        return 'blocks'

    def field_selection_name(self) -> str:
        return 'block'

    def primary_key(self) -> tuple[str, ...]:
        return 'number',

    def field_weights(self) -> dict[str, int]:
        return {
            'digest': 2
        }


class _REvents(RTable):
    def table_name(self) -> str:
        return 'events'

    def columns(self) -> tuple[str, ...]:
        return 'index', 'extrinsic_index', 'call_address'

    def where(self, builder: Builder, req: EventRequest):
        yield builder.in_condition('name', req.get('name'))


class _SEvents(STable):
    def table_name(self) -> str:
        return 'events'

    def field_selection_name(self) -> str:
        return 'event'

    def primary_key(self) -> tuple[str, ...]:
        return 'index',

    def field_weights(self) -> dict[str, int]:
        return {
            'args': 4
        }


class _RCalls(RTable):
    def table_name(self) -> str:
        return 'calls'

    def columns(self) -> tuple[str, ...]:
        return 'extrinsic_index', 'address'

    def where(self, builder: Builder, req: CallRequest):
        yield builder.in_condition('name', req.get('name'))


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


def _build_model() -> Model:
    r_events = _REvents()
    r_calls = _RCalls()

    s_events = _SEvents()
    s_calls = _SCalls()
    s_extrinsics = _SExtrinsics()

    s_events.sources.extend([
        r_events,
        JoinRel(
            table=r_calls,
            include_flag_name='events',
            join_condition='s.extrinsic_index = r.extrinsic_index AND s.call_address = r.address'
        )
    ])

    s_calls.sources.extend([
        r_calls,
        RefRel(
            table=r_events,
            include_flag_name='call',
            key=['extrinsic_index', 'call_address']
        ),
        JoinRel(
            table=r_calls,
            include_flag_name='stack',
            join_condition='s.extrinsic_index = r.extrinsic_index AND '
                           'len(s.address) < len(r.address) AND '
                           's.address = r.address[1:len(s.address)]'
        ),
        JoinRel(
            table=r_calls,
            include_flag_name='subcalls',
            join_condition='s.extrinsic_index = r.extrinsic_index AND '
                           'len(s.address) > len(r.address) AND '
                           's.address[1:len(r.address)] = r.address'
        )
    ])

    s_extrinsics.sources.extend([
        RefRel(
            table=r_events,
            include_flag_name='extrinsic',
            key=['extrinsic_index']
        ),
        RefRel(
            table=r_calls,
            include_flag_name='extrinsic',
            key=['extrinsic_index']
        )
    ])

    return [
        _BlocksTable(),
        r_events,
        r_calls,
        s_events,
        s_calls,
        s_extrinsics
    ]


MODEL = _build_model()
