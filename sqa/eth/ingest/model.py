from typing import TypedDict, Optional, Literal, NotRequired, Union


Bytes = str
Bytes4 = str
Bytes8 = str
Bytes32 = str
Address20 = str
Hash32 = str
Qty = str


class Block(TypedDict):
    number: Qty
    hash: Hash32
    parentHash: Hash32
    timestamp: Qty
    transactionsRoot: Hash32
    receiptsRoot: Hash32
    stateRoot: Hash32
    logsBloom: Bytes
    sha3Uncles: Hash32
    extraData: Bytes
    miner: Address20
    nonce: NotRequired[Bytes8]
    mixHash: NotRequired[Bytes]
    size: Qty
    gasLimit: Qty
    gasUsed: Qty
    difficulty: NotRequired[Qty]
    totalDifficulty: NotRequired[Qty]
    baseFeePerGas: NotRequired[Qty]
    uncles: list[Hash32]
    transactions: list['Transaction']
    logs_: NotRequired[list['Log']]
    unknownTraceReplays_: NotRequired[list]


# Alternative syntax allows to use reserved keywords as keys
Transaction = TypedDict('Transaction', {
    'blockHash': Hash32,
    'blockNumber': Qty,
    'transactionIndex': Qty,
    'hash': Hash32,
    'nonce': Qty,
    'from': Address20,
    'to': Optional[Address20],
    'input': Bytes,
    'value': Qty,
    'type': Qty,
    'gas': Qty,
    'gasPrice': Qty,
    'maxFeePerGas': NotRequired[Qty],
    'maxPriorityFeePerGas': NotRequired[Qty],
    'v': NotRequired[Qty],
    'r': NotRequired[Bytes32],
    's': NotRequired[Bytes32],
    'yParity': NotRequired[Qty],
    'accessList': NotRequired[list],
    'chainId': NotRequired[Qty],
    'receipt_': NotRequired['Receipt'],
    'debugFrame_': NotRequired['DebugFrameResult'],
    'debugStateDiff_': NotRequired['DebugStateDiffResult'],
    'traceReplay_': NotRequired['TraceTransactionReplay']
})


class Log(TypedDict):
    blockHash: Hash32
    blockNumber: Qty
    logIndex: Qty
    transactionIndex: Qty
    transactionHash: Hash32
    address: Address20
    data: Bytes
    topics: list[Bytes32]


class Receipt(TypedDict):
    transactionHash: Hash32
    transactionIndex: Qty
    blockHash: Hash32
    blockNumber: Qty
    cumulativeGasUsed: Qty
    effectiveGasPrice: NotRequired[Qty]
    gasUsed: Qty
    contractAddress: NotRequired[Address20]
    logs: list[Log]
    type: Qty
    status: NotRequired[Qty]


DebugFrame = TypedDict('DebugFrame', {
    'type': Literal['CALL', 'STATICCALL', 'DELEGATECALL', 'CREATE', 'CREATE2', 'SELFDESTRUCT', 'STOP'],
    'from': Address20,
    'to': Address20,
    'value': NotRequired[Qty],
    'gas': Qty,
    'gasUsed': Qty,
    'input': Bytes,
    'output': Bytes,
    'error': NotRequired[str],
    'revertReason': NotRequired[str],
    'calls': NotRequired[list['DebugFrame']]
})


class DebugFrameResult(TypedDict):
    result: DebugFrame


class DebugStateMap(TypedDict, total=False):
    balance: Qty
    code: Bytes
    nonce: int
    storage: dict[Bytes32, Bytes]


class DebugStateDiff(TypedDict):
    pre: dict[Address20, DebugStateMap]
    post: dict[Address20, DebugStateMap]


class DebugStateDiffResult(TypedDict):
    result: DebugStateDiff


# https://github.com/openethereum/parity-ethereum/blob/55c90d4016505317034e3e98f699af07f5404b63/rpc/src/v1/types/trace.rs#L482
class _TraceRecBase(TypedDict):
    error: NotRequired[str]
    traceAddress: list[int]
    subtraces: int


class TraceCreateRec(_TraceRecBase):
    type: Literal['create']
    action: 'TraceCreateAction'
    result: NotRequired['TraceCreateActionResult']


TraceCreateAction = TypedDict('TraceCreateAction', {
    'from': Address20,
    'value': Qty,
    'gas': Qty,
    'init': Bytes,
    'creation_method': NotRequired[Literal['create', 'create2']]
})


class TraceCreateActionResult(TypedDict):
    gasUsed: Qty
    code: Bytes
    address: Address20


class TraceCallRec(_TraceRecBase):
    type: Literal['call']
    action: 'TraceCallAction'
    result: 'TraceCallActionResult'


TraceCallAction = TypedDict('TraceCallAction', {
    'from': Address20,
    'to': Address20,
    'value': Qty,
    'gas': Qty,
    'input': Bytes,
    'callType': Optional[Literal['call', 'callcode', 'delegatecall', 'staticcall']]
})


class TraceCallActionResult(TypedDict):
    gasUsed: Qty
    output: Bytes


class TraceSuicideRec(_TraceRecBase):
    type: Literal['suicide']
    action: 'TraceSuicideAction'


class TraceSuicideAction(TypedDict):
    address: Address20
    refundAddress: Address20
    balance: Qty


# is not supported for trace_replayBlockTransactions rpc call
class TraceRewardRec(_TraceRecBase):
    type: Literal['reward']
    action: 'TraceRewardAction'


class TraceRewardAction(TypedDict):
    author: Address20
    value: Qty
    rewardType: Literal['block', 'uncle', 'emptyStep', 'external']


TraceRec = Union[TraceCreateRec, TraceCallRec, TraceSuicideRec, TraceRewardRec]


TraceAddDiff = TypedDict('TraceAddDiff', {
    '+': Bytes
})


TraceChangeValue = TypedDict('TraceChangeValue', {
    'from': Bytes,
    'to': Bytes,
})


TraceChangeDiff = TypedDict('TraceChangeDiff', {
    '*': TraceChangeValue
})


TraceDeleteDiff = TypedDict('TraceDeleteDiff', {
    '-': Bytes
})


TraceDiff = Union[Literal['='], TraceAddDiff, TraceChangeDiff, TraceDeleteDiff]


class TraceStateDiff(TypedDict):
    balance: TraceDiff
    code: TraceDiff
    nonce: TraceDiff
    storage: dict[Bytes32, TraceDiff]


class TraceTransactionReplay(TypedDict):
    transactionHash: NotRequired[Hash32]
    trace: NotRequired[list[TraceRec]]
    stateDiff: NotRequired[dict[Address20, TraceStateDiff]]
    index_: NotRequired[int]
