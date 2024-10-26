from typing import TypedDict, Any, NotRequired


JsBigInt = str


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


class TransactionResult(TypedDict):
    contractRet: NotRequired[str]


class Transaction(TypedDict):
    hash: str
    transactionIndex: int
    ret: NotRequired[list[TransactionResult]]
    signature: NotRequired[list[str]]
    type: str
    parameter: Any
    permissionId: NotRequired[int]
    refBlockBytes: NotRequired[str]
    refBlockHash: NotRequired[str]
    feeLimit: NotRequired[JsBigInt]
    expiration: NotRequired[int]
    timestamp: NotRequired[JsBigInt]
    rawDataHex: str
    fee: NotRequired[JsBigInt]
    contractResult: NotRequired[str]
    contractAddress: NotRequired[str]
    resMessage: NotRequired[str]
    withdrawAmount: NotRequired[JsBigInt]
    unfreezeAmount: NotRequired[JsBigInt]
    withdrawExpireAmount: NotRequired[JsBigInt]
    cancelUnfreezeV2Amount: NotRequired[dict[str, JsBigInt]]
    result: NotRequired[str]
    energyFee: NotRequired[JsBigInt]
    energyUsage: NotRequired[JsBigInt]
    energyUsageTotal: NotRequired[JsBigInt]
    netUsage: NotRequired[JsBigInt]
    netFee: NotRequired[JsBigInt]
    originEnergyUsage: NotRequired[JsBigInt]
    energyPenaltyTotal: NotRequired[JsBigInt]


class CallValueInfo(TypedDict):
    callValue: NotRequired[JsBigInt]
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
