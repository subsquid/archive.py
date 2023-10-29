from typing import NamedTuple, Iterable, TypeVar, Generic, Protocol

import pyarrow.dataset

from sqa.layout import Partition
from .util import to_snake_case, json_project


ColumnName = str
ReqName = str
R = TypeVar('R')
FieldSelection = dict[str, dict[str, bool]]


class Table(NamedTuple):
    name: str
    primary_key: list[ColumnName, ...]
    column_weights: dict[ColumnName, ColumnName | int] = {}


class Scan(Generic[R]):
    def table(self) -> Table:
        raise NotImplementedError()

    def request_name(self) -> ReqName:
        return self.table().name

    def where(self, item_req: R) -> Iterable[pyarrow.dataset.Expression | None]:
        raise NotImplementedError()


class ScanQuery(Protocol):
    def include_columns(self, columns: list[ColumnName]) -> None:
        pass


ScanQueries = dict[ReqName, list[ScanQuery]]
ScanData = dict[ReqName, list[pyarrow.Table]]


class ItemSrcQuery(Protocol):
    def fetch(self, partition: Partition, scan_data: ScanData) -> pyarrow.Table:
        pass


class ItemSrc(Protocol):
    def get_queries(self, req: dict, scan_queries: ScanQueries) -> Iterable[ItemSrcQuery]:
        pass


class RefRel(NamedTuple):
    scan: Scan
    include_flag_name: str
    scan_columns: list[ColumnName]


class JoinRel(NamedTuple):
    scan: Scan
    include_flag_name: str
    query: str
    scan_columns: list[ColumnName] | None = None


class SubRel(NamedTuple):
    scan: Scan
    scan_columns: list[ColumnName]
    include_flag_name: str


class Item:
    sources: list[Scan | RefRel | JoinRel | ItemSrc]

    def __init__(self):
        self.sources = []

    def table(self) -> Table:
        raise NotImplementedError()

    def name(self) -> str:
        return self.table().name

    def get_selected_fields(self, fields: FieldSelection) -> list[str]:
        raise NotImplementedError()

    def selected_columns(self, fields: FieldSelection) -> list[str]:
        return list(to_snake_case(f) for f in self.get_selected_fields(fields))

    def project(self, fields: FieldSelection) -> str:
        return json_project(self.get_selected_fields(fields))


Model = list[Scan | Item]
