import re
from typing import Any, Iterable

import pyarrow.dataset
import pyarrow.compute


Exp = str


class Params:
    def __init__(self):
        self.list = []
        self.variables = {}

    def new_param(self, value: Any) -> Exp:
        self.list.append(value)
        return f'${len(self.list)}'

    def new_variable(self, name: str) -> Exp:
        assert name not in self.variables
        self.list.append(None)
        idx = len(self.list)
        self.variables[name] = idx - 1
        return f'${idx}'

    def set_var(self, name: str, value: Any) -> None:
        idx = self.variables[name]
        self.list[idx] = value


def include_columns(columns: list[str], columns_to_include: Iterable[str] | None) -> None:
    if columns_to_include is None:
        return
    included = set(columns)
    for c in columns_to_include:
        if c in included:
            pass
        else:
            columns.append(c)


def to_snake_case(name: str) -> str:
    name = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', name).lower()


def remove_camel_prefix(name: str, prefix: str) -> str:
    return name[len(prefix)].lower() + name[len(prefix) + 1:]


def project(columns: Iterable[str], prefix: str = '') -> str:
    return ', '.join(
        prefix + c for c in columns
    )


def join_condition(columns: Iterable[str], left: str, right: str) -> str:
    return ' AND '.join(
        f'{left}.{c} = {right}.{c}' for c in columns
    )


def json_project(fields: Iterable[str | tuple[str, str]]) -> str:
    props = []
    for alias in fields:
        if isinstance(alias, tuple):
            exp = alias[1]
            alias = alias[0]
        else:
            exp = f'"{to_snake_case(alias)}"'
        props.append(f"'{alias}'")
        props.append(exp)

    return f'json_object({", ".join(props)})'


def get_selected_fields(
        fields: dict[str, bool] | None,
        required_fields: list[str] | None = None
) -> list[str]:
    ls = list(required_fields) if required_fields else []
    if fields:
        include_columns(ls, (f for f, on in fields.items() if on))
    return ls


def field_in(field_name: str, value_list: list[Any] | None) -> pyarrow.dataset.Expression | None:
    if value_list is None:
        return
    if len(value_list) == 0:
        return pyarrow.compute.scalar(0) == pyarrow.compute.scalar(1)
    elif len(value_list) == 1:
        return pyarrow.compute.field(field_name) == pyarrow.compute.scalar(value_list[0])
    else:
        return pyarrow.compute.field(field_name).isin(value_list)
