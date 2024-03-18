from typing import Optional, TypedDict, NotRequired


FELT = str  # NOTE: cairo bytes32
Hash32 = FELT
Qty = str  # For numerical quantities, represented as strings for precision


class _ResourcePrice(TypedDict):
    price_in_fri: FELT
    price_in_wei: FELT


class Block(TypedDict):
    block_number: int
    block_hash: Hash32
    parent_hash: Hash32

    status: str  # status enum
    new_root: Hash32
    timestamp: int
    sequencer_address: FELT

    transactions: list['Transaction']

    # NOTE: fields below from schema, havent been received from node at the time of writing
    starknet_version: str
    l1_gas_price: _ResourcePrice

    # external fields
    events: NotRequired[list['Event']]


class WriterBlock(Block):
    number: int  # renamed from block_number for consistency
    hash: Hash32  # renamed from block_hash for consistency

    # external fields
    writer_txs: list['WriterTransaction']
    writer_events: list['WriterEvent']


class ResourceBounds(TypedDict):
    # NOTE: int64
    max_amount: Qty
    max_price_per_unit: Qty


class _ResourceBoundsMap(TypedDict):
    l1_gas: ResourceBounds
    l2_gas: ResourceBounds


class Transaction(TypedDict):
    transaction_hash: Hash32

    contract_address: Optional[FELT]  # Address of the contract for contract-related transactions
    entry_point_selector: Optional[FELT]
    calldata: Optional[list[FELT]]
    max_fee: Optional[FELT]
    version: str
    signature: Optional[list[FELT]]
    nonce: Optional[FELT]
    type: str  # transaction type enum
    sender_address: Optional[FELT]
    class_hash: Optional[Hash32]
    compiled_class_hash: Optional[Hash32]
    contract_address_salt: Optional[FELT]
    constructor_calldata: Optional[list[str]]
    # NOTE: fields below from schema, havent been received from node at the time of writing
    resource_bounds: Optional[_ResourceBoundsMap]
    tip: Optional[FELT]
    paymaster_data: Optional[list[FELT]]
    account_deployment_data: Optional[list[FELT]]
    nonce_data_availability_mode: Optional[str]  # Nonce DA mode enum
    fee_data_availability_mode: Optional[str]  # Fee DA mode enum


class WriterTransaction(Transaction):
    transaction_index: int
    block_number: int


class ActualFee(TypedDict):
    amount: Qty
    unit: str  # Enum: WEI, FRI


class MessageToL1(TypedDict):
    from_address: FELT
    to_address: FELT
    payload: list[FELT]


class EventContent(TypedDict):
    from_address: FELT
    keys: list[FELT]
    data: list[FELT]


class ExecutionResources(TypedDict):
    steps: int
    memory_holes: int
    range_check_builtin_applications: int
    pedersen_builtin_applications: int
    poseidon_builtin_applications: int
    ec_op_builtin_applications: int
    ecdsa_builtin_applications: int
    bitwise_builtin_applications: int
    keccak_builtin_applications: int
    segment_arena_builtin: int


class Receipt(TypedDict, total=False):
    transaction_hash: Hash32
    actual_fee: ActualFee
    execution_status: str  # Enum: SUCCEEDED, REVERTED
    finality_status: str  # Enum: ACCEPTED_ON_L2, ACCEPTED_ON_L1
    block_hash: Hash32
    block_number: int
    messages_sent: list[MessageToL1]
    revert_reason: Optional[str]
    events: list[EventContent]
    execution_resources: ExecutionResources
    type: str  # Enum: INVOKE, L1_HANDLER, DECLARE, DEPLOY_ACCOUNT
    contract_address: Optional[FELT]  # For DEPLOY_ACCOUNT receipts
    message_hash: Optional[Hash32]  # For L1_HANDLER receipts


class Event(TypedDict):
    block_number: int
    block_hash: Hash32
    transaction_hash: Hash32

    from_address: FELT
    keys: list[FELT]
    data: list[FELT]


class EventPage(TypedDict):
    events: list[Event]
    continuation_token: Optional[str]


class WriterEvent(Event):
    transaction_index: int
    event_index: int
