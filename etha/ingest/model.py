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
    nonce: Bytes8
    mixHash: Bytes
    size: Qty
    gasLimit: Qty
    gasUsed: Qty
    difficulty: Qty
    totalDifficulty: Qty
    uncles: list[Hash32]
    transactions: list['Transaction']
    logs_: NotRequired[list['Log']]
    trace_: NotRequired[list['Trace']]


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
    'v': Qty,
    'r': Bytes32,
    's': Bytes32,
    'receipt_': NotRequired['Receipt']
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
    status: Qty


# https://github.com/openethereum/parity-ethereum/blob/55c90d4016505317034e3e98f699af07f5404b63/rpc/src/v1/types/trace.rs#L482
class _TraceBase(TypedDict):
    error: NotRequired[str]
    traceAddress: list[int]
    subtraces: int
    blockNumber: int
    blockHash: Hash32


class CreateTrace(_TraceBase):
    type: Literal['create']
    action: 'CreateTraceAction'
    result: NotRequired['CreateTraceActionResult']
    transactionIndex: int
    transactionHash: Hash32


CreateTraceAction = TypedDict('CreateTraceAction', {
    'from': Address20,
    'value': Qty,
    'gas': Qty,
    'init': Bytes,
    'creation_method': NotRequired[Union[Literal['create'], Literal['create2']]]
})


class CreateTraceActionResult(TypedDict):
    gasUsed: Qty
    code: Bytes
    address: Address20


class CallTrace(_TraceBase):
    type: Literal['call']
    action: 'CallTraceAction'
    result: 'CallTraceActionResult'
    transactionIndex: int
    transactionHash: Hash32


CallTraceAction = TypedDict('CallTraceAction', {
    'from': Address20,
    'to': Address20,
    'value': Qty,
    'gas': Qty,
    'input': Bytes,
    'callType': Optional[Union[Literal['call'], Literal['callcode'], Literal['delegatecall'], Literal['staticcall']]]
})


class CallTraceActionResult(TypedDict):
    gasUsed: Qty
    output: Bytes


class SuicideTrace(_TraceBase):
    type: Literal['suicide']
    action: 'SuicideTraceAction'
    transactionIndex: int
    transactionHash: Hash32


class SuicideTraceAction(TypedDict):
    address: Address20
    refundAddress: Address20
    balance: Qty


class RewardTrace(_TraceBase):
    type: Literal['reward']
    action: 'RewardTraceAction'


class RewardTraceAction(TypedDict):
    author: Address20
    value: Qty
    rewardType: Union[Literal['block'], Literal['uncle'], Literal['emptyStep'], Literal['external']]


Trace = Union[CreateTrace, CallTrace, SuicideTrace, RewardTrace]
