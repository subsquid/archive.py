from typing import NamedTuple, Protocol, Union, TypeVar, Generic, Iterable, Any

from .util import WhereExp, json_project, to_snake_case


R = TypeVar('R')


class Table(Generic[R]):
    relations: list[Union['RefRel', 'JoinRel']]

    def __init__(self):
        self.relations = []

    def table_name(self) -> str:
        raise NotImplementedError()

    def request_name(self) -> str:
        return self.table_name()

    def selection_name(self) -> str:
        raise NotImplementedError()

    def primary_key(self) -> tuple[str, ...]:
        raise NotImplementedError()

    def key(self) -> tuple[str, ...]:
        return self.primary_key()

    def primary_key_columns(self) -> tuple[str, ...]:
        return tuple(map(to_snake_case, self.primary_key()))

    def key_columns(self) -> tuple[str, ...]:
        return tuple(map(to_snake_case, self.key()))

    def where(self, builder: 'Builder', req: R) -> Iterable[WhereExp | None]:
        raise NotImplementedError()

    def project(self, fields: dict, prefix: str = '') -> str:
        return json_project(self.get_selected_fields(fields), prefix=prefix)

    def get_weight(self, fields: dict) -> int:
        weights = self.field_weights()
        fields = self.get_selected_fields(fields)
        return sum(weights.get(f, 1) for f in fields)

    def field_weights(self) -> dict[str, int]:
        return {}

    def get_selected_fields(self, fields: dict) -> list[str]:
        ls = list(self.key())
        seen = set(self.key())
        for f, on in fields.items():
            if on and f not in seen:
                ls.append(f)
                seen.add(f)
        return ls


class RefRel(NamedTuple):
    table: Table
    include_flag_name: str
    key: list[str]


class JoinRel(NamedTuple):
    table: Table
    include_flag_name: str
    join_condition: str


class Builder(Protocol):
    def new_param(self, value: Any) -> str:
        pass

    def in_condition(self, col: str, variants: list | None) -> WhereExp | None:
        pass


Model = list[Table]
