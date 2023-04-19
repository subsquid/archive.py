from typing import NotRequired, TypedDict

import marshmallow as mm
import marshmallow.validate


class BlockFieldSelection(TypedDict, total=False):
    number: bool
    hash: bool
    parentHash: bool
    timestamp: bool
    transactionsRoot: bool
    receiptsRoot: bool
    stateRoot: bool
    logsBloom: bool
    sha3Uncles: bool
    extraData: bool
    miner: bool
    nonce: bool
    mixHash: bool
    size: bool
    gasLimit: bool
    gasUsed: bool
    difficulty: bool
    totalDifficulty: bool
    baseFeePerGas: bool


TxFieldSelection = TypedDict('TxFieldSelection', {
    'transactionIndex': bool,
    'hash': bool,
    'nonce': bool,
    'from': bool,
    'to': bool,
    'input': bool,
    'value': bool,
    'gas': bool,
    'gasPrice': bool,
    'maxFeePerGas': bool,
    'maxPriorityFeePerGas': bool,
    'v': bool,
    'r': bool,
    's': bool,
    'yParity': bool,
    'chainId': bool,
    'sighash': bool,
    'gasUsed': bool,
    'cumulativeGasUsed': bool,
    'effectiveGasUsed': bool,
    'type': bool,
    'status': bool
}, total=False)


class LogFieldSelection(TypedDict, total=False):
    logIndex: bool
    transactionIndex: bool
    transactionHash: bool
    address: bool
    data: bool
    topics: bool
    transaction: bool


class FieldSelection(TypedDict):
    block: NotRequired[BlockFieldSelection]
    transaction: NotRequired[TxFieldSelection]
    log: NotRequired[LogFieldSelection]


class LogFilter(TypedDict):
    address: NotRequired[list[str]]
    topic0: NotRequired[list[str]]


class TxFilter(TypedDict):
    to: NotRequired[list[str]]
    sighash: NotRequired[list[str]]


class Query(TypedDict):
    fromBlock: int
    toBlock: NotRequired[int]
    includeAllBlocks: NotRequired[bool]
    fields: NotRequired[FieldSelection]
    logs: NotRequired[list[LogFilter]]
    transactions: NotRequired[list[TxFilter]]


def _field_map(typed_dict):
    return mm.fields.Dict(
        mm.fields.Str(validate=lambda k: k in typed_dict.__optional_keys__),
        mm.fields.Boolean(),
        required=False
    )


class FieldSelectionSchema(marshmallow.Schema):
    block = _field_map(BlockFieldSelection)
    transaction = _field_map(TxFieldSelection)
    log = _field_map(LogFieldSelection)


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

    includeAllBlocks = mm.fields.Boolean(required=False)

    fields = mm.fields.Nested(FieldSelectionSchema())

    logs = mm.fields.List(mm.fields.Nested(LogFilterSchema()))

    transactions = mm.fields.List(mm.fields.Nested(TxFilterSchema()))


query_schema = QuerySchema()
