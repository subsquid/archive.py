from typing import TypedDict, Any, NotRequired, Literal


Base58Bytes = str
JSON = Any


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
    index: int
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
    fee: int
    loadedAddresses: LoadedAddresses


class Instruction(TypedDict):
    transactionIndex: int
    instructionAddress: list[int]
    programId: Base58Bytes
    accounts: list[Base58Bytes]
    data: Base58Bytes
    error: NotRequired[str]


class LogMessage(TypedDict):
    transactionIndex: int
    logIndex: int
    instructionAddress: list[int]
    programId: Base58Bytes
    kind: Literal['log', 'data', 'other']
    message: str


class Block(TypedDict):
    header: BlockHeader
    transactions: list[Transaction]
    instructions: list[Instruction]
    logs: list[LogMessage]
