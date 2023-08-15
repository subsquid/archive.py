import re
from typing import NamedTuple, Union, Iterable


class And(NamedTuple):
    ops: list['WhereExp']


class Or(NamedTuple):
    ops: list['WhereExp']


class Bin(NamedTuple):
    op: str
    lhs: str
    rhs: str


WhereExp = Union[And, Or, Bin]


def print_where(exp: WhereExp) -> str:
    if isinstance(exp, Bin):
        return f"{exp.lhs} {exp.op} {exp.rhs}"
    elif isinstance(exp, And):
        return ' AND '.join(f"({e})" for e in (print_where(op) for op in exp.ops) if e)
    elif isinstance(exp, Or):
        return ' OR '.join(f"({e})" for e in (print_where(op) for op in exp.ops) if e)
    else:
        raise ValueError


def to_snake_case(name: str) -> str:
    name = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', name).lower()


def remove_camel_prefix(name: str, prefix: str) -> str:
    return name[len(prefix)].lower() + name[len(prefix) + 1:]


class SelectBuilder:
    def __init__(self, table: str):
        self._table = table
        self._columns = []
        self._where = And([])

    def add_columns(self, cols: Iterable[str]) -> None:
        self._columns.extend(cols)

    def add_where(self, exp: WhereExp) -> None:
        self._where.ops.append(exp)

    def build(self) -> str:
        assert self._columns

        sql = f"SELECT {', '.join(self._columns)} FROM {self._table}"

        where = print_where(self._where)
        if where:
            sql += f" WHERE {where}"

        return sql


def json_project(fields: Iterable[str | tuple[str, str]], prefix: str = '') -> str:
    props = []
    for alias in fields:
        if isinstance(alias, tuple):
            exp = alias[1]
            alias = alias[0]
        else:
            exp = f'{prefix}"{to_snake_case(alias)}"'

        props.append(f"'{alias}'")
        props.append(exp)

    return f'json_object({", ".join(props)})'


def project(columns: Iterable[str], prefix: str = '') -> str:
    return ', '.join(
        prefix + c for c in columns
    )


def compute_item_weight(fields: Iterable[str], weights: dict[str, int]) -> int:
    return sum(weights.get(f, 1) for f in fields)


def union_all(relations: Iterable[str]) -> str:
    return ' UNION ALL '.join(relations)
