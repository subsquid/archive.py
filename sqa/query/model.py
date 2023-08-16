from typing import NamedTuple, Protocol, Union, TypeVar, Generic, Iterable, Any

from .util import WhereExp, json_project, to_snake_case


R = TypeVar('R')


class RTable(Generic[R]):
    def table_name(self) -> str:
        raise NotImplementedError()

    def request_name(self) -> str:
        return self.table_name()

    def columns(self) -> tuple[str, ...]:
        raise NotImplementedError()

    def where(self, builder: 'Builder', req: R) -> Iterable[WhereExp | None]:
        raise NotImplementedError()


class STable:
    sources: list[Union[RTable, 'RefRel', 'JoinRel']]

    def __init__(self):
        self.sources = []

    def table_name(self) -> str:
        raise NotImplementedError()

    def prop_name(self) -> str:
        return self.table_name()

    def field_selection_name(self) -> str:
        raise NotImplementedError()

    def primary_key(self) -> tuple[str, ...]:
        raise NotImplementedError()

    def primary_key_columns(self) -> tuple[str, ...]:
        return tuple(map(to_snake_case, self.primary_key()))

    def project(self, fields: dict, prefix: str = '') -> str:
        return json_project(self.get_selected_fields(fields), prefix=prefix)

    def get_weight(self, fields: dict) -> int:
        weights = self.field_weights()
        fields = self.get_selected_fields(fields)
        return sum(weights.get(f, 1) for f in fields)

    def field_weights(self) -> dict[str, int]:
        return {}

    def get_selected_fields(self, fields: dict) -> list[str]:
        ls = list(self.required_fields())
        seen = set(self.required_fields())
        for f, on in fields.items():
            if on and f not in seen:
                ls.append(f)
                seen.add(f)
        return ls

    def required_fields(self) -> tuple[str, ...]:
        return self.primary_key()


class RefRel(NamedTuple):
    table: RTable
    include_flag_name: str
    key: list[str]


class JoinRel(NamedTuple):
    table: RTable
    include_flag_name: str
    join_condition: str


class Builder(Protocol):
    def new_param(self, value: Any) -> str:
        pass

    def in_condition(self, col: str, variants: list | None) -> WhereExp | None:
        pass


Model = list[STable | RTable]
