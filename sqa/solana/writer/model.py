from typing import TypedDict, Any, NotRequired, Literal


Base58Bytes = str
JSON = Any
JsBigInt = str


class BlockHeader(TypedDict):
    hash: Base58Bytes
    height: int
    slot: int
    parentSlot: int
    parentHash: Base58Bytes
    timestamp: int


class AddressTableLookup(TypedDict):
    accountKey: Base58Bytes
    readonlyIndexes: list[int]
    writableIndexes: list[int]


class LoadedAddresses(TypedDict):
    readonly: list[Base58Bytes]
    writable: list[Base58Bytes]


class Transaction(TypedDict):
    transactionIndex: int
    version: Literal['legacy'] | int
    accountKeys: list[Base58Bytes]
    addressTableLookups: list[AddressTableLookup]
    numReadonlySignedAccounts: int
    numReadonlyUnsignedAccounts: int
    numRequiredSignatures: int
    recentBlockhash: Base58Bytes
    signatures: list[Base58Bytes]
    err: NotRequired[JSON]
    computeUnitsConsumed: NotRequired[int]
    fee: JsBigInt
    loadedAddresses: LoadedAddresses
    hasDroppedLogMessages: bool


class Instruction(TypedDict):
    transactionIndex: int
    instructionAddress: list[int]
    programId: Base58Bytes
    accounts: list[Base58Bytes]
    data: Base58Bytes
    computeUnitsConsumed: NotRequired[JsBigInt]
    error: NotRequired[str]
    isCommitted: bool
    hasDroppedLogMessages: bool


class LogMessage(TypedDict):
    transactionIndex: int
    logIndex: int
    instructionAddress: list[int]
    programId: Base58Bytes
    kind: Literal['log', 'data', 'other']
    message: str


class Balance(TypedDict):
    transactionIndex: int
    account: Base58Bytes
    pre: JsBigInt
    post: JsBigInt


class TokenBalance(TypedDict):
    transactionIndex: int
    account: Base58Bytes
    mint: Base58Bytes
    decimals: int
    programId: NotRequired[Base58Bytes]
    preOwner: NotRequired[Base58Bytes]
    postOwner: NotRequired[Base58Bytes]
    pre: NotRequired[JsBigInt]
    post: NotRequired[JsBigInt]


class Reward(TypedDict):
    pubkey: Base58Bytes
    lamports: JsBigInt
    postBalance: JsBigInt
    rewardType: NotRequired[str]
    commission: NotRequired[str]


class Block(TypedDict):
    header: BlockHeader
    transactions: list[Transaction]
    instructions: list[Instruction]
    logs: list[LogMessage]
    balances: list[Balance]
    tokenBalances: list[TokenBalance]
    rewards: list[Reward]

