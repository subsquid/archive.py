from typing import TypedDict, Any, NotRequired, Literal


Bytes = str
BigInt = str
JSON = Any


class BlockHeader(TypedDict):
    height: int
    hash: Bytes
    parentHash: Bytes
    stateRoot: Bytes
    extrinsicsRoot: Bytes
    digest: JSON
    specName: str
    specVersion: int
    implName: str
    implVersion: int
    timestamp: NotRequired[int]
    validator: NotRequired[Bytes]


class Extrinsic(TypedDict):
    index: int
    version: int
    signature: NotRequired[JSON]
    fee: NotRequired[BigInt]
    tip: NotRequired[BigInt]
    error: NotRequired[JSON]
    success: bool
    hash: Bytes


class Call(TypedDict):
    extrinsicIndex: int
    address: list[int]
    name: str
    args: JSON
    origin: NotRequired[JSON]
    error: NotRequired[JSON]
    success: bool
    _ethereumTransactTo: NotRequired[Bytes]
    _ethereumTransactSighash: NotRequired[Bytes]


class Event(TypedDict):
    index: int
    name: str
    args: JSON
    phase: Literal['Initialization', 'ApplyExtrinsic', 'Finalization']
    extrinsicIndex: NotRequired[int]
    callAddress: NotRequired[list[int]]
    _evmLogAddress: NotRequired[Bytes]
    _evmLogTopics: NotRequired[list[Bytes]]
    _contractAddress: NotRequired[Bytes]
    _gearProgramId: NotRequired[Bytes]


class Block(TypedDict):
    header: BlockHeader
    extrinsics: list[Extrinsic]
    calls: list[Call]
    events: list[Event]
