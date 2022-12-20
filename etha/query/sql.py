import re
from typing import Any, NamedTuple, Union

from etha.query.model import FieldMap


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


class SqlQuery(NamedTuple):
    sql: str
    params: list[Any]

    def set_file(self, name: str):
        self.params[0] = name


class SqlBuilder:
    def __init__(self):
        self._params = ['*']
        self._columns = []
        self._where = And([])

    def set_file(self, name: str):
        self._params[0] = name

    def add_columns(self, fields: Union[FieldMap, list[str], None]):
        if isinstance(fields, dict):
            ls = [f'"{to_snake_case(k)}"' for k, include in fields.items() if include]
            ls.sort()
            self._columns.extend(ls)
        elif fields:
            self._columns.extend(to_snake_case(f) for f in fields)

    def add_where(self, exp: WhereExp):
        self._where.ops.append(exp)

    def param(self, value) -> str:
        self._params.append(value)
        return f"?{len(self._params)}"

    def build(self) -> SqlQuery:
        assert self._columns
        cols = set(self._columns)
        sql = f"SELECT {', '.join(c for c in self._columns if c in cols)} FROM read_parquet(?1)"

        where = print_where(self._where)
        if where:
            sql += f" WHERE {where}"

        return SqlQuery(sql, self._params)


def to_snake_case(name):
    name = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', name).lower()
