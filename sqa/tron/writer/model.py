from typing import TypedDict, Any, NotRequired


class BlockHeader(TypedDict):
    height: int
    hash: str
    parentHash: str
    txTrieRoot: str
    version: int
    timestamp: int
    witnessAddress: str
    witnessSignature: str


class Log(TypedDict):
    logIndex: int
    transactionHash: str
    address: str
    data: str
    topics: list[str]


class Transaction(TypedDict):
    hash: str
    ret: NotRequired[str]
    signature: list[str]
    type: str
    parameter: Any
    permissionId: NotRequired[int]
    refBlockBytes: str
    refBlockHash: str
    feeLimit: NotRequired[int]
    expiration: int
    timestamp: NotRequired[int]
    rawDataHex: str
    fee: NotRequired[int]
    contractResult: NotRequired[str]
    contractAddress: NotRequired[str]
    resMessage: NotRequired[str]
    withdrawAmount: NotRequired[int]
    unfreezeAmount: NotRequired[int]
    withdrawExpireAmount: NotRequired[int]
    cancelUnfreezeV2Amount: NotRequired[dict[str, int]]
    result: NotRequired[str]
    energyFee: NotRequired[int]
    energyUsage: NotRequired[int]
    energyUsageTotal: NotRequired[int]
    netUsage: NotRequired[int]
    netFee: NotRequired[int]
    originEnergyUsage: NotRequired[int]
    energyPenaltyTotal: NotRequired[int]


class CallValueInfo(TypedDict):
    callValue: NotRequired[int]
    tokenId: NotRequired[str]


class InternalTransaction(TypedDict):
    transactionHash: str
    hash: str
    callerAddress: str
    transferToAddress: str
    callValueInfo: list[CallValueInfo]
    note: str
    rejected: NotRequired[bool]
    extra: NotRequired[str]


class Block(TypedDict):
    header: BlockHeader
    logs: list[Log]
    transactions: list[Transaction]
    internalTransactions: list[InternalTransaction]
