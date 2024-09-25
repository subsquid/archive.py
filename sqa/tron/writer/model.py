from typing import TypedDict, Any, NotRequired


class BlockHeader(TypedDict):
    height: int
    hash: str
    parentHash: str
    txTrieRoot: str
    version: NotRequired[int]
    timestamp: int
    witnessAddress: str
    witnessSignature: NotRequired[str]


class Log(TypedDict):
    logIndex: int
    transactionIndex: int
    address: str
    data: NotRequired[str]
    topics: NotRequired[list[str]]


class Transaction(TypedDict):
    hash: str
    transactionIndex: int
    ret: NotRequired[str]
    signature: NotRequired[list[str]]
    type: str
    parameter: Any
    permissionId: NotRequired[int]
    refBlockBytes: NotRequired[str]
    refBlockHash: NotRequired[str]
    feeLimit: NotRequired[int]
    expiration: NotRequired[int]
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
    transactionIndex: int
    internalTransactionIndex: int
    hash: str
    callerAddress: str
    transferToAddress: NotRequired[str]
    callValueInfo: list[CallValueInfo]
    note: str
    rejected: NotRequired[bool]
    extra: NotRequired[str]


class Block(TypedDict):
    header: BlockHeader
    logs: list[Log]
    transactions: list[Transaction]
    internalTransactions: list[InternalTransaction]
