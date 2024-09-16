from typing import TypedDict, NotRequired, Literal


Bytes = str
JsBigInt = str


class BlockHeader(TypedDict):
    hash: Bytes
    height: int
    daHeight: JsBigInt
    transactionsRoot: Bytes
    transactionsCount: int
    messageReceiptCount: int
    prevRoot: Bytes
    time: JsBigInt
    applicationHash: Bytes
    eventInboxRoot: Bytes
    consensusParametersVersion: int
    stateTransitionBytecodeVersion: int
    messageOutboxRoot: Bytes


class Policies(TypedDict):
    tip: NotRequired[JsBigInt]
    witnessLimit: NotRequired[JsBigInt]
    maturity: NotRequired[int]
    maxFee: NotRequired[JsBigInt]


class ProgramState(TypedDict):
    returnType: Literal['RETURN', 'RETURN_DATA', 'REVERT']
    data: Bytes


class SubmittedStatus(TypedDict):
    type: Literal['SubmittedStatus']
    time: JsBigInt


class SuccessStatus(TypedDict):
    type: Literal['SuccessStatus']
    transactionId: Bytes
    time: JsBigInt
    programState: NotRequired[ProgramState]
    totalGas: JsBigInt
    totalFee: JsBigInt


class SqueezedOutStatus(TypedDict):
    type: Literal['SqueezedOutStatus']
    reason: str


class FailureStatus(TypedDict):
    type: Literal['FailureStatus']
    transactionId: Bytes
    time: JsBigInt
    reason: str
    programState: NotRequired[ProgramState]
    totalGas: JsBigInt
    totalFee: JsBigInt


Status = SubmittedStatus | SuccessStatus | SqueezedOutStatus | FailureStatus


class ConsensusParametersPurpose(TypedDict):
    type: Literal['ConsensusParametersPurpose']
    witnessIndex: int
    checksum: Bytes


class StateTransitionPurpose:
    type: Literal['StateTransitionPurpose']
    root: Bytes


UpgradePurpose = ConsensusParametersPurpose | StateTransitionPurpose


class TransactionInputContract(TypedDict):
    utxoId: Bytes
    balanceRoot: Bytes
    stateRoot: Bytes
    txPointer: str
    contractId: Bytes


class OutputContract(TypedDict):
    inputIndex: int
    balanceRoot: Bytes
    stateRoot: Bytes


class Transaction(TypedDict):
    index: int
    hash: Bytes
    inputAssetIds: NotRequired[list[Bytes]]
    inputContracts: NotRequired[list[Bytes]]
    inputContract: NotRequired[TransactionInputContract]
    policies: NotRequired[Policies]
    scriptGasLimit: NotRequired[JsBigInt]
    maturity: NotRequired[int]
    mintAmount: NotRequired[JsBigInt]
    mintAssetId: NotRequired[Bytes]
    mintGasPrice: NotRequired[JsBigInt]
    txPointer: NotRequired[str]
    isScript: bool
    isCreate: bool
    isMint: bool
    isUpgrade: bool
    isUpload: bool
    type: NotRequired[Literal['Script', 'Create', 'Mint']]
    outputContract: NotRequired[OutputContract]
    witnesses: NotRequired[list[Bytes]]
    receiptsRoot: NotRequired[Bytes]
    status: Status
    script: NotRequired[Bytes]
    scriptData: NotRequired[Bytes]
    bytecodeWitnessIndex: NotRequired[int]
    bytecodeRoot: NotRequired[Bytes]
    salt: NotRequired[Bytes]
    storageSlots: NotRequired[list[Bytes]]
    rawPayload: NotRequired[Bytes]
    subsectionIndex: NotRequired[int]
    subsectionsNumber: NotRequired[int]
    proofSet: NotRequired[list[Bytes]]
    upgradePurpose: NotRequired[UpgradePurpose]


class InputCoin(TypedDict):
    type: Literal['InputCoin']
    index: int
    transactionIndex: int
    utxoId: Bytes
    owner: Bytes
    amount: JsBigInt
    assetId: Bytes
    txPointer: str
    witnessIndex: int
    predicateGasUsed: JsBigInt
    predicate: Bytes
    predicateData: Bytes


class InputContract(TypedDict):
    type: Literal['InputContract']
    index: int
    transactionIndex: int
    utxoId: Bytes
    balanceRoot: Bytes
    stateRoot: Bytes
    txPointer: str
    contractId: Bytes


class InputMessage(TypedDict):
    type: Literal['InputMessage']
    index: int
    transactionIndex: int
    sender: Bytes
    recipient: Bytes
    amount: JsBigInt
    nonce: Bytes
    witnessIndex: int
    predicateGasUsed: JsBigInt
    data: Bytes
    predicate: Bytes
    predicateData: Bytes


TransactionInput = InputCoin | InputContract | InputMessage


class CoinOutput(TypedDict):
    type: Literal['CoinOutput']
    index: int
    transactionIndex: int
    to: Bytes
    amount: JsBigInt
    assetId: Bytes


class ContractOutput(TypedDict):
    type: Literal['ContractOutput']
    index: int
    transactionIndex: int
    inputIndex: int
    balanceRoot: Bytes
    stateRoot: Bytes


class ChangeOutput(TypedDict):
    type: Literal['ChangeOutput']
    index: int
    transactionIndex: int
    to: Bytes
    amount: JsBigInt
    assetId: Bytes


class VariableOutput(TypedDict):
    type: Literal['VariableOutput']
    index: int
    transactionIndex: int
    to: Bytes
    amount: JsBigInt
    assetId: Bytes


class ContractCreated(TypedDict):
    type: Literal['ContractCreated']
    index: int
    transactionIndex: int
    contract: Bytes
    stateRoot: Bytes


TransactionOutput = CoinOutput | ContractOutput | ChangeOutput | VariableOutput | ContractCreated


ReceiptType = Literal['CALL', 'RETURN', 'RETURN_DATA', 'PANIC', 'REVERT', 'LOG', 'LOG_DATA', 'TRANSFER', 'TRANSFER_OUT', 'SCRIPT_RESULT', 'MESSAGE_OUT', 'MINT', 'BURN']


Receipt = TypedDict('Receipt', {
    'index': int,
    'transactionIndex': int,
    'contract': NotRequired[Bytes],
    'pc': NotRequired[JsBigInt],
    'is': NotRequired[JsBigInt],
    'to': NotRequired[Bytes],
    'toAddress': NotRequired[Bytes],
    'amount': NotRequired[JsBigInt],
    'assetId': NotRequired[Bytes],
    'gas': NotRequired[JsBigInt],
    'param1': NotRequired[JsBigInt],
    'param2': NotRequired[JsBigInt],
    'val': NotRequired[JsBigInt],
    'ptr': NotRequired[JsBigInt],
    'digest': NotRequired[Bytes],
    'reason': NotRequired[JsBigInt],
    'ra': NotRequired[JsBigInt],
    'rb': NotRequired[JsBigInt],
    'rc': NotRequired[JsBigInt],
    'rd': NotRequired[JsBigInt],
    'len': NotRequired[JsBigInt],
    'receiptType': ReceiptType,
    'result': NotRequired[JsBigInt],
    'gasUsed': NotRequired[JsBigInt],
    'data': NotRequired[Bytes],
    'sender': NotRequired[Bytes],
    'recipient': NotRequired[Bytes],
    'nonce': NotRequired[Bytes],
    'contractId': NotRequired[Bytes],
    'subId': NotRequired[Bytes],
})


class Block(TypedDict):
    header: BlockHeader
    transactions: list[Transaction]
    inputs: list[TransactionInput]
    outputs: list[TransactionOutput]
    receipts: list[Receipt]
