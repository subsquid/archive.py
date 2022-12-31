from typing import NotRequired, TypedDict

import marshmallow as mm
import marshmallow.validate


FieldMap = dict[str, bool]


class FieldSelection(TypedDict):
    block: NotRequired[FieldMap]
    transaction: NotRequired[FieldMap]
    log: NotRequired[FieldMap]


class LogFilter(TypedDict):
    address: NotRequired[list[str]]
    topic0: NotRequired[list[str]]


class TxFilter(TypedDict):
    to: NotRequired[list[str]]
    sighash: NotRequired[list[str]]


class Query(TypedDict):
    fromBlock: int
    toBlock: NotRequired[int]
    fields: NotRequired[FieldSelection]
    logs: NotRequired[list[LogFilter]]
    transactions: NotRequired[list[TxFilter]]


class FieldSelectionSchema(marshmallow.Schema):
    block = mm.fields.Dict(mm.fields.Str(), mm.fields.Boolean())
    transaction = mm.fields.Dict(mm.fields.Str(), mm.fields.Boolean())
    log = mm.fields.Dict(mm.fields.Str(), mm.fields.Boolean())


class LogFilterSchema(mm.Schema):
    address = mm.fields.List(mm.fields.Str())
    topic0 = mm.fields.List(mm.fields.Str())


class TxFilterSchema(mm.Schema):
    to = mm.fields.List(mm.fields.Str())
    sighash = mm.fields.List(mm.fields.Str())


class QuerySchema(mm.Schema):
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

    fields = mm.fields.Nested(FieldSelectionSchema())

    logs = mm.fields.List(mm.fields.Nested(LogFilterSchema()))

    transactions = mm.fields.List(mm.fields.Nested(TxFilterSchema()))


query_schema = QuerySchema()
