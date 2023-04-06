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
    'gas': Qty,
    'gasPrice': Qty,
    'maxFeePerGas': NotRequired[Qty],
    'maxPriorityFeePerGas': NotRequired[Qty],
    'v': NotRequired[Qty],
    'r': NotRequired[Bytes32],
    's': NotRequired[Bytes32],
    'yParity': NotRequired[Qty],
    'chainId': NotRequired[Qty],
    'receipt_': NotRequired['Receipt'],
    'replay_': NotRequired['TransactionReplay'],
})


class Log(TypedDict):
    blockHash: Hash32
    blockNumber: Qty
    logIndex: Qty
    transactionIndex: Qty
    address: Address20
    data: Bytes
    topics: list[Bytes32]


class Receipt(TypedDict):
    transactionHash: Hash32
    transactionIndex: Qty
    blockHash: Hash32
    blockNumber: Qty
    cumulativeGasUsed: Qty
    effectiveGasPrice: Qty
    gasUsed: Qty
    logs: list[Log]
    type: Qty
    status: NotRequired[Qty]


# https://github.com/openethereum/parity-ethereum/blob/55c90d4016505317034e3e98f699af07f5404b63/rpc/src/v1/types/trace.rs#L482
class _TraceBase(TypedDict):
    error: NotRequired[str]
    traceAddress: list[int]
    subtraces: int


class CreateTrace(_TraceBase):
    type: Literal['create']
    action: 'CreateTraceAction'
    result: NotRequired['CreateTraceActionResult']


CreateTraceAction = TypedDict('CreateTraceAction', {
    'from': Address20,
    'value': Qty,
    'gas': Qty,
    'init': Bytes,
    'creation_method': NotRequired[Literal['create', 'create2']]
})


class CreateTraceActionResult(TypedDict):
    gasUsed: Qty
    code: Bytes
    address: Address20


class CallTrace(_TraceBase):
    type: Literal['call']
    action: 'CallTraceAction'
    result: 'CallTraceActionResult'


CallTraceAction = TypedDict('CallTraceAction', {
    'from': Address20,
    'to': Address20,
    'value': Qty,
    'gas': Qty,
    'input': Bytes,
    'callType': Optional[Literal['call', 'callcode', 'delegatecall', 'staticcall']]
})


class CallTraceActionResult(TypedDict):
    gasUsed: Qty
    output: Bytes


class SuicideTrace(_TraceBase):
    type: Literal['suicide']
    action: 'SuicideTraceAction'


class SuicideTraceAction(TypedDict):
    address: Address20
    refundAddress: Address20
    balance: Qty


# is not supported for trace_replayBlockTransactions rpc call
class RewardTrace(_TraceBase):
    type: Literal['reward']
    action: 'RewardTraceAction'


class RewardTraceAction(TypedDict):
    author: Address20
    value: Qty
    rewardType: Literal['block', 'uncle', 'emptyStep', 'external']


Trace = Union[CreateTrace, CallTrace, SuicideTrace, RewardTrace]


AddDiff = TypedDict('AddDiff', {
    '+': Bytes
})


ChangeValue = TypedDict('ChangeValue', {
    'from': Bytes,
    'to': Bytes,
})


ChangeDiff = TypedDict('ChangeDiff', {
    '*': ChangeValue
})


DeleteDiff = TypedDict('DeleteDiff', {
    '-': Bytes
})


Diff = Union[Literal['='], AddDiff, ChangeDiff, DeleteDiff]


class StateDiff(TypedDict):
    balance: Diff
    code: Diff
    nonce: Diff
    storage: dict[Bytes32, Diff]


class TransactionReplay(TypedDict):
    stateDiff: dict[Address20, StateDiff]
    trace: list[Trace]
    transactionHash: Hash32
