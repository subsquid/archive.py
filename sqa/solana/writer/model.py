from typing import TypedDict, Any, NotRequired, Literal


Base58Bytes = str
JSON = Any


class BlockHeader(TypedDict):
    height: int
    slot: int
    hash: Base58Bytes
    parentSlot: int
    parentHash: Base58Bytes
    timestamp: int


class Transaction(TypedDict):
    index: int
    version: Literal['legacy'] | int
    accountKeys: list[Base58Bytes]
    addressTableLookups: list[JSON]
    numReadonlySignedAccounts: int
    numReadonlyUnsignedAccounts: int
    numRequiredSignatures: int
    recentBlockhash: Base58Bytes
    signatures: list[Base58Bytes]
    err: NotRequired[JSON]
    computeUnitsConsumed: int
    fee: int
    logMessages: list[str]
    loadedAddresses: JSON


class Instruction(TypedDict):
    transactionIndex: int
    instructionAddress: list[int]
    programId: Base58Bytes
    accounts: list[Base58Bytes]
    data: Base58Bytes


class Block(TypedDict):
    header: BlockHeader
    transactions: list[Transaction]
    instructions: list[Instruction]
