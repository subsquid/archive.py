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


class TraceFieldSelection(TypedDict, total=False):
    traceAddress: bool
    subtraces: bool
    transactionIndex: bool
    type: bool
    error: bool
    createFrom: bool
    createValue: bool
    createGas: bool
    createInit: bool
    createResultGasUsed: bool
    createResultCode: bool
    createResultAddress: bool
    callFrom: bool
    callTo: bool
    callValue: bool
    callGas: bool
    callInput: bool
    callSighash: bool
    callType: bool
    callResultGasUsed: bool
    callResultOutput: bool
    suicideAddress: bool
    suicideRefundAddress: bool
    suicideBalance: bool
    rewardAuthor: bool
    rewardValue: bool
    rewardType: bool


class StateDiffFieldSelection(TypedDict, total=False):
    transactionIndex: bool
    address: bool
    key: bool
    kind: bool
    prev: bool
    next: bool


class FieldSelection(TypedDict, total=False):
    block: BlockFieldSelection
    transaction: TxFieldSelection
    log: LogFieldSelection
    trace: TraceFieldSelection
    stateDiff: StateDiffFieldSelection


class LogRequest(TypedDict, total=False):
    address: list[str]
    topic0: list[str]
    transaction: bool


TxRequest = TypedDict('TxRequest', {
    'from': list[str],
    'to': list[str],
    'sighash': list[str],
    'logs': bool,
    'traces': bool,
    'stateDiffs': bool
}, total=False)


class TraceRequest(TypedDict, total=False):
    type: list[str]
    createFrom: list[str]
    callFrom: list[str]
    callTo: list[str]
    callSighash: list[str]
    suicideRefundAddress: list[str]
    rewardAuthor: list[str]
    transaction: bool
    subtraces: bool


class StateDiffRequest(TypedDict, total=False):
    address: list[str]
    key: list[str]
    kind: list[str]
    transaction: bool


class Query(TypedDict):
    fromBlock: int
    toBlock: NotRequired[int]
    includeAllBlocks: NotRequired[bool]
    fields: NotRequired[FieldSelection]
    logs: NotRequired[list[LogRequest]]
    transactions: NotRequired[list[TxRequest]]
    traces: NotRequired[list[TraceRequest]]
    stateDiffs: NotRequired[list[StateDiffRequest]]


def _field_map_schema(typed_dict):
    return mm.fields.Dict(
        mm.fields.Str(validate=lambda k: k in typed_dict.__optional_keys__),
        mm.fields.Boolean(),
        required=False
    )


class FieldSelectionSchema(mm.Schema):
    block = _field_map_schema(BlockFieldSelection)
    transaction = _field_map_schema(TxFieldSelection)
    log = _field_map_schema(LogFieldSelection)
    trace = _field_map_schema(TraceFieldSelection)
    stateDiff = _field_map_schema(StateDiffFieldSelection)


class LogRequestSchema(mm.Schema):
    address = mm.fields.List(mm.fields.Str())
    topic0 = mm.fields.List(mm.fields.Str())
    transaction = mm.fields.Boolean()


TxRequestSchema = mm.Schema.from_dict({
    'from': mm.fields.List(mm.fields.Str()),
    'to': mm.fields.List(mm.fields.Str()),
    'sighash': mm.fields.List(mm.fields.Str()),
    'logs': mm.fields.Boolean(),
    'traces': mm.fields.Boolean(),
    'stateDiffs': mm.fields.Boolean()
})


class TraceRequestSchema(mm.Schema):
    type = mm.fields.List(mm.fields.Str())
    createFrom = mm.fields.List(mm.fields.Str())
    callFrom = mm.fields.List(mm.fields.Str())
    callTo = mm.fields.List(mm.fields.Str())
    callSighash = mm.fields.List(mm.fields.Str())
    suicideRefundAddress = mm.fields.List(mm.fields.Str())
    rewardAuthor = mm.fields.List(mm.fields.Str())
    transaction = mm.fields.Boolean()
    subtraces = mm.fields.Boolean()


class StateDiffRequestSchema(mm.Schema):
    address = mm.fields.List(mm.fields.Str())
    key = mm.fields.List(mm.fields.Str())
    kind = mm.fields.List(mm.fields.Str())
    transaction =  mm.fields.Boolean()


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

    logs = mm.fields.List(mm.fields.Nested(LogRequestSchema()))

    transactions = mm.fields.List(mm.fields.Nested(TxRequestSchema()))

    traces = mm.fields.List(mm.fields.Nested(TraceRequestSchema()))

    stateDiffs = mm.fields.List(mm.fields.Nested(StateDiffRequestSchema()))


query_schema = QuerySchema()
