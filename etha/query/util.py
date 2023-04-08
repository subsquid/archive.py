import re
from typing import NamedTuple, Union


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


class SqlBuilder:
    def __init__(self, table: str):
        self._table = table
        self._columns = []
        self._where = And([])

    def add_columns(self, cols: list[str]) -> None:
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
