from typing import Optional, TypedDict


FieldMap = dict[str, bool]


class FieldSelection(TypedDict):
    block: Optional[FieldMap]
    transaction: Optional[FieldMap]
    log: Optional[FieldMap]


class LogFilter(TypedDict):
    address: Optional[list[str]]
    topic0: Optional[list[str]]


class TxFilter(TypedDict):
    to: Optional[list[str]]
    sighash: Optional[list[str]]


class Query(TypedDict):
    fromBlock: int
    toBlock: Optional[int]
    fields: FieldSelection
    logs: Optional[list[LogFilter]]
    transactions: Optional[list[TxFilter]]


