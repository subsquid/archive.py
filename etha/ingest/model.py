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
    'callTrace_': NotRequired['CallFrame'],
    'stateDiff_': NotRequired['StateDiff']
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
    effectiveGasPrice: Qty
    gasUsed: Qty
    contractAddress: NotRequired[Address20]
    logs: list[Log]
    type: Qty
    status: NotRequired[Qty]


CallFrame = TypedDict('CallFrame', {
    'type': Literal['CALL', 'STATICCALL', 'DELEGATECALL', 'CREATE', 'CREATE2', 'SELFDESTRUCT'],
    'from': Address20,
    'to': Address20,
    'value': NotRequired[Qty],
    'gas': Qty,
    'gasUsed': Qty,
    'input': Bytes,
    'output': Bytes,
    'error': NotRequired[str],
    'revertReason': NotRequired[str],
    'calls': NotRequired[list['CallFrame']]
})


class CallFrameResult(TypedDict):
    result: CallFrame


class StateMap(TypedDict, total=False):
    balance: Qty
    code: Bytes
    nonce: int
    storage: dict[Bytes32, Bytes]


class StateDiff(TypedDict):
    pre: dict[Address20, StateMap]
    post: dict[Address20, StateMap]


class StateDiffResult(TypedDict):
    result: StateDiff
