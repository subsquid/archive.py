from typing import Optional, TypedDict, List, NotRequired


FELT = str  # NOTE: cairo bytes32
Hash32 = FELT
Qty = str  # For numerical quantities, represented as strings for precision

class Block(TypedDict):
    status: str  # status enum
    block_hash: Hash32
    parent_hash: Hash32
    number: int  # renamed from block_number for consistency
    new_root: Hash32
    timestamp: int
    sequencer_address: FELT
    # TODO: fields from schema, havent been received
    # starknet_version: str
    # l1_gas_price: _L1GasPrice
    transactions: List['Transaction']

    # external fields
    logs_: NotRequired[list['Event']]

class Transaction(TypedDict):
    block_number: int
    transaction_hash: Hash32
    contract_address: Optional[FELT]  # Address of the contract for contract-related transactions
    entry_point_selector: Optional[FELT]
    calldata: Optional[List[FELT]]
    max_fee: Optional[FELT]
    version: str
    signature: Optional[List[FELT]]
    nonce: Optional[FELT]
    type: str  # transaction type enum
    sender_address: Optional[FELT]
    class_hash: Optional[Hash32]
    compiled_class_hash: Optional[Hash32]
    contract_address_salt: Optional[FELT]
    constructor_calldata: Optional[List[str]]
    # TODO: fields from schema, havent been received
    # resource_bounds: _ResourceBounds
    # tip: FELT
    # paymaster_data: Optional[list[FELT]]
    # account_deployment_data: Optional[list[FELT]]
    # nonce_data_availability_mode: str  # Nonce DA mode enum
    # fee_data_availability_mode: str  # Fee DA mode enum


class ActualFee(TypedDict):
    amount: Qty
    unit: str  # Enum: WEI, FRI

class MessageToL1(TypedDict):
    from_address: FELT
    to_address: FELT
    payload: List[FELT]

class EventContent(TypedDict):
    from_address: FELT
    keys: List[FELT]
    data: List[FELT]


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
    messages_sent: List[MessageToL1]
    revert_reason: Optional[str]
    events: List[EventContent]
    execution_resources: ExecutionResources
    type: str  # Enum: INVOKE, L1_HANDLER, DECLARE, DEPLOY_ACCOUNT
    contract_address: Optional[FELT]  # For DEPLOY_ACCOUNT receipts
    message_hash: Optional[Hash32]  # For L1_HANDLER receipts

class Event(TypedDict):
    from_address: FELT
    keys: List[FELT]
    data: List[FELT]
    block_hash: Hash32
    block_number: int
    transaction_hash: Hash32

class EventPage(TypedDict):
    events: List[Event]
    continuation_token: Optional[str]
