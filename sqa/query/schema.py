from typing import TypedDict, NotRequired

import marshmallow as mm
import marshmallow.validate

from .model import FieldSelection


class ArchiveQuery(TypedDict):
    fromBlock: int
    toBlock: NotRequired[int]
    includeAllBlocks: NotRequired[bool]
    fields: NotRequired[FieldSelection]


class BaseQuerySchema(mm.Schema):
    type = mm.fields.Str()

    fromBlock = mm.fields.Integer(
        required=True,
        strict=True,
        validate=mm.validate.Range(min=0, min_inclusive=True)
    )

    toBlock = mm.fields.Integer(
        required=False,
        strict=True,
        validate=mm.validate.Range(min=0, min_inclusive=True)
    )

    includeAllBlocks = mm.fields.Boolean(required=False)


def field_map_schema(typed_dict):
    return mm.fields.Dict(
        mm.fields.Str(validate=lambda k: k in typed_dict.__optional_keys__),
        mm.fields.Boolean(),
        required=False
    )
