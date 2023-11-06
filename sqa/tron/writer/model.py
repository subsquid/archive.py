from typing import TypedDict, Any, NotRequired


class Log(TypedDict):
    address: str
    data: NotRequired[str]
    topics: list[str]


class ContractParameter(TypedDict):
    value: Any
    type_url: str


class Contract(TypedDict):
    parameter: ContractParameter
    type: str
    Permission_id: NotRequired[int]


class TransactionRawData(TypedDict):
    contract: list[Contract]
    ref_block_bytes: str
    ref_block_hash: str
    expiration: int
    fee_limit: NotRequired[int]
    timestamp: NotRequired[int]


class TransactionReceipt(TypedDict):
    result: NotRequired[str]
    energy_fee: NotRequired[int]
    energy_usage: NotRequired[int]
    energy_usage_total: NotRequired[int]
    net_usage: NotRequired[int]
    net_fee: NotRequired[int]
    origin_energy_usage: NotRequired[int]
    energy_penalty_total: NotRequired[int]


class CallValueInfo(TypedDict):
    callValue: NotRequired[int]
    tokenId: NotRequired[str]


class InternalTransaction(TypedDict):
    hash: str
    caller_address: str
    transferTo_address: str
    callValueInfo: list[CallValueInfo]
    note: str
    rejected: NotRequired[bool]
    extra: NotRequired[str]


class TransactionInfo(TypedDict):
    id: str
    fee: NotRequired[int]
    blockNumber: int
    blockTimeStamp: int
    contractResult: list[str]
    contract_address: NotRequired[str]
    receipt: TransactionReceipt
    log: NotRequired[list[Log]]
    result: NotRequired[str]
    resMessage: NotRequired[str]
    withdraw_amount: NotRequired[int]
    unfreeze_amount: NotRequired[int]
    internal_transactions: NotRequired[list[InternalTransaction]]
    withdraw_expire_amount: NotRequired[int]
    cancel_unfreezeV2_amount: NotRequired[dict[str, int]]


class TransactionResult(TypedDict):
    contractRet: str


class Transaction(TypedDict):
    ret: list[TransactionResult]
    signature: list[str]
    txID: str
    raw_data: TransactionRawData
    raw_data_hex: str
    info: NotRequired[TransactionInfo]


class BlockRawData(TypedDict):
    number: NotRequired[int]
    txTrieRoot: str
    witness_address: str
    parentHash: str
    version: int
    timestamp: int


class BlockHeader(TypedDict):
    raw_data: BlockRawData
    witness_signature: str


class Block(TypedDict):
    blockID: str
    block_header: BlockHeader
    transactions: NotRequired[list[Transaction]]


class BlockData(TypedDict):
    height: int
    hash: str
    block: Block
