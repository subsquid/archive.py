from typing import TypedDict, Optional


Bytes = str
Bytes4 = str
Bytes8 = str
Bytes32 = str
Address20 = str
Hash32 = str
HexNumber = str


class BlockHeader(TypedDict):
    number: int
    hash: Hash32
    parentHash: Hash32
    nonce: Optional[Bytes8]
    sha3Uncles: Hash32
    logsBloom: Bytes
    transactionsRoot: Hash32
    stateRoot: Hash32
    receiptsRoot: Hash32
    miner: Address20
    gasUsed: int
    gasLimit: int
    size: int
    timestamp: int
    extraData: Bytes
    difficulty: Optional[int]
    totalDifficulty: Optional[int]
    mixHash: Optional[Bytes]
    baseFeePerGas: Optional[int]


# Alternative syntax allows to use reserved keywords as keys
Transaction = TypedDict(
    'Transaction',
    {
        'blockNumber': int,
        'transactionIndex': int,
        'hash': Hash32,
        'gas': int,
        'gasPrice': Optional[int],
        'maxFeePerGas': Optional[int],
        'maxPriorityFeePerGas': Optional[int],
        'from': Address20,
        'to': Optional[Address20],
        'sighash': Optional[Bytes4],
        'input': Bytes,
        'nonce': int,
        'value': int,
        'type': int,
        'v': Optional[str],
        'r': str,
        's': str,
        'yParity': Optional[int],
        'chainId': Optional[int],
    }
)


class Log(TypedDict):
    blockNumber: int
    logIndex: int
    transactionIndex: int
    address: Address20
    data: Bytes
    topic0: Optional[Bytes32]
    topic1: Optional[Bytes32]
    topic2: Optional[Bytes32]
    topic3: Optional[Bytes32]
    removed: bool


class Block(TypedDict):
    header: BlockHeader
    transactions: list[Transaction]
    logs: list[Log]
