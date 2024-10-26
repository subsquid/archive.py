from typing import NotRequired, TypedDict

FELT = str  # NOTE: cairo bytes32
STD_HASH = FELT
Qty = str  # For numerical quantities, represented as strings for precision


class _ResourcePrice(TypedDict):
    price_in_fri: FELT
    price_in_wei: FELT


class Block(TypedDict):
    block_number: int
    block_hash: STD_HASH
    parent_hash: STD_HASH

    status: str  # status enum
    new_root: STD_HASH
    timestamp: int
    sequencer_address: FELT

    transactions: list['Transaction']

    # NOTE: fields below from schema, havent been received from node at the time of writing
    starknet_version: str
    l1_gas_price: _ResourcePrice

    # external fields
    events: NotRequired[list['Event']]
    traces: NotRequired[list['Trace']]
    state_update: NotRequired['BlockStateUpdate']


class WriterBlock(Block):
    number: int  # renamed from block_number for consistency
    hash: STD_HASH  # renamed from block_hash for consistency

    # external fields
    writer_txs: list['WriterTransaction']
    writer_events: list['WriterEvent']
    writer_call_traces: NotRequired[list['WriterCall']]
    # NOTE: call messages extracted from call traces
    writer_state_update: NotRequired['WriterBlockStateUpdate']
    writer_storage_diffs: NotRequired[list['WriterStorageDiffItem']]


class ResourceBounds(TypedDict):
    # NOTE: int64
    max_amount: Qty
    max_price_per_unit: Qty


class _ResourceBoundsMap(TypedDict):
    l1_gas: ResourceBounds
    l2_gas: ResourceBounds


class Transaction(TypedDict):
    transaction_hash: STD_HASH

    contract_address: NotRequired[FELT]  # Address of the contract for contract-related transactions
    entry_point_selector: NotRequired[FELT]
    calldata: NotRequired[list[FELT]]
    max_fee: NotRequired[FELT]
    version: str
    signature: NotRequired[list[FELT]]
    nonce: NotRequired[FELT]
    type: str  # transaction type enum
    sender_address: NotRequired[FELT]
    class_hash: NotRequired[STD_HASH]
    compiled_class_hash: NotRequired[STD_HASH]
    contract_address_salt: NotRequired[FELT]
    constructor_calldata: NotRequired[list[str]]
    # NOTE: fields below from schema, havent been received from node at the time of writing
    resource_bounds: NotRequired[_ResourceBoundsMap]
    tip: NotRequired[FELT]
    paymaster_data: NotRequired[list[FELT]]
    account_deployment_data: NotRequired[list[FELT]]
    nonce_data_availability_mode: NotRequired[str]  # Nonce DA mode enum
    fee_data_availability_mode: NotRequired[str]  # Fee DA mode enum


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
    transaction_hash: STD_HASH
    actual_fee: ActualFee
    execution_status: str  # Enum: SUCCEEDED, REVERTED
    finality_status: str  # Enum: ACCEPTED_ON_L2, ACCEPTED_ON_L1
    block_hash: STD_HASH
    block_number: int
    messages_sent: list[MessageToL1]
    revert_reason: NotRequired[str]
    events: list[EventContent]
    execution_resources: ExecutionResources
    type: str  # Enum: INVOKE, L1_HANDLER, DECLARE, DEPLOY_ACCOUNT
    contract_address: NotRequired[FELT]  # For DEPLOY_ACCOUNT receipts
    message_hash: NotRequired[STD_HASH]  # For L1_HANDLER receipts


class Event(TypedDict):
    block_number: int
    block_hash: STD_HASH
    transaction_hash: STD_HASH

    from_address: FELT
    keys: list[FELT]
    data: list[FELT]


class EventPage(TypedDict):
    events: list[Event]
    continuation_token: NotRequired[str]


class WriterEvent(Event):
    transaction_index: int
    event_index: int


class CallMessage(TypedDict):
    """
    The messages sent by this invocation to L1
    """

    from_address: FELT  # l2_address in spec
    to_address: FELT  # l1_address in spec
    payload: list[FELT]
    order: int


class WriterCallMessage(CallMessage):
    block_number: int
    transaction_index: int
    trace_address: list[int]


class CallEvent(TypedDict):
    """
    The events emitted in this invocation
    """

    keys: list[FELT]
    data: list[FELT]
    order: int


class Call(TypedDict):
    caller_address: NotRequired[FELT]
    contract_address: NotRequired[FELT]
    call_type: NotRequired[str]
    class_hash: NotRequired[FELT]
    entry_point_selector: NotRequired[FELT]
    entry_point_type: NotRequired[str]
    revert_reason: NotRequired[str]
    calldata: list[FELT]
    result: list[FELT]
    calls: list['Call']
    events: list[CallEvent]
    messages: list[CallMessage]
    execution_resources: ExecutionResources


class WriterCall(Call):
    block_number: int
    transaction_index: int
    trace_type: str
    invocation_type: str

    trace_address: list[int]


class TraceRoot(TypedDict):
    type: str
    execution_resources: ExecutionResources
    execute_invocation: NotRequired[Call]
    constructor_invocation: NotRequired[Call]
    validate_invocation: NotRequired[Call]
    fee_transfer_invocation: NotRequired[Call]


class Trace(TypedDict):
    trace_root: TraceRoot
    transaction_hash: FELT


class StorageEntry(TypedDict):
    key: FELT
    value: FELT


class StorageDiffItem(TypedDict):
    address: FELT
    storage_entries: list[StorageEntry]


class WriterStorageDiffItem(StorageDiffItem):
    block_number: int


class DeclaredContractHash(TypedDict):
    class_hash: FELT
    compiled_class_hash: FELT


class DeployedContract(TypedDict):
    address: FELT
    class_hash: FELT


class ReplacedClass(TypedDict):
    contract_address: FELT
    class_hash: FELT


class ContractsNonce(TypedDict):
    contract_address: FELT
    nonce: FELT


class StateDiff(TypedDict):
    storage_diffs: list[StorageDiffItem]
    deprecated_declared_classes: list[FELT]
    declared_classes: list[DeclaredContractHash]
    deployed_contracts: list[DeployedContract]
    replaced_classes: list[ReplacedClass]
    nonces: list[ContractsNonce]


class BlockStateUpdate(TypedDict):
    block_hash: NotRequired[STD_HASH]
    new_root: NotRequired[FELT]
    old_root: FELT
    state_diff: StateDiff


class WriterBlockStateUpdate(BlockStateUpdate):
    block_number: int
