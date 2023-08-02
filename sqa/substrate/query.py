from typing import TypedDict

import marshmallow as mm

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
