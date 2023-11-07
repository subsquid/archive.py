import itertools
from typing import TypedDict, NotRequired, Any, Iterable

from . import SqlQuery
from .model import STable, RefRel, JoinRel, Model, RTable
from .util import SelectBuilder, And, Or, union_all, Bin, project, json_project


class MissingData(Exception):
    pass


class ArchiveQuery(TypedDict):
    fromBlock: int
    toBlock: NotRequired[int]
    includeAllBlocks: NotRequired[bool]
    fields: NotRequired[dict[str, dict[str, bool]]]


def build_sql_query(
        model: Model,
        q: ArchiveQuery,
        filelist: list[str],
        size_limit=1_000_000
) -> SqlQuery:

    builder = _SqlQueryBuilder(
        model,
        q,
        filelist,
        size_limit
    )

    sql = builder.build()

    return SqlQuery(
        sql=sql,
        params=builder.params,
        variables=builder.variables,
        tables=builder.tables,
        last_block=q.get('toBlock')
    )


class _SqlQueryBuilder:
    def __init__(self,
                 model: Model,
                 q: ArchiveQuery,
                 filelist: list[str],
                 size_limit=1_000_000
                 ):
        self.model = model
        self.q = q
        self.filelist = filelist
        self.tables = []
        self.params = []
        self.variables = {}
        self.relations = {}
        self.size_limit = self.new_param(size_limit)
        self.first_block = self.new_param(self.q.get('fromBlock'))
        self.last_block = self.new_variable('last_block')

    def new_param(self, value: Any) -> str:
        self.params.append(value)
        return f'${len(self.params)}'

    def new_variable(self, name: str) -> str:
        assert name not in self.variables
        idx = int(self.new_param(None)[1:])
        self.variables[name] = idx - 1
        return f'${idx}'

    def in_condition(self, col: str, variants: list | None) -> Bin | None:
        if variants is None:
            return None
        elif len(variants) == 0:
            return Bin('=', '1', '0')
        elif len(variants) == 1:
            return Bin('=', col, self.new_param(variants[0]))
        else:
            return Bin('IN', col, f"(SELECT UNNEST({self.new_param(variants)}))")

    def use_table(self, name: str) -> None:
        if name not in self.relations:
            file = f'{name}.parquet'
            if file in self.filelist:
                self.tables.append(name)
                self.relations[name] = f'SELECT * FROM read_parquet({self.new_variable(name)})'
            else:
                raise MissingData(
                    f'"{name}" data is not supported by this archive on requested block range'
                )

    def add_req(self, table: RTable) -> None:
        requests: list[dict] | None
        requests = self.q.get(table.request_name())
        if not requests:
            return

        self.use_table(table.table_name())

        include_flags = list(self.get_include_flags(table))

        def relation_mask(req: dict):
            return tuple(
                req.get(flag, False) for flag in include_flags
            )

        union = []
        requests = requests.copy()
        requests.sort(key=relation_mask)

        for mask, req_group in itertools.groupby(requests, key=relation_mask):
            qb = SelectBuilder(table.table_name())
            qb.add_columns(['block_number'])
            qb.add_columns(table.columns())
            for i, flag in enumerate(include_flags):
                qb.add_columns([
                    f'{self.new_param(mask[i])} AS include_{flag}'
                ])

            cases = []
            for req in req_group:
                conditions = list(c for c in table.where(self, req) if c)
                if conditions:
                    cases.append(And(conditions))
                else:
                    cases = []
                    break
            if cases:
                qb.add_where(Or(cases))

            union.append(qb.build())

        relation_name = f'requested_{table.request_name()}'

        if len(union) == 1:
            self.relations[relation_name] = union[0]
        else:
            qb = SelectBuilder(f'({union_all(union)})')
            qb.add_columns(['block_number'])
            qb.add_columns(table.columns())
            qb.add_columns(
                f'MAX(include_{flag}) AS include_{flag}'
                for flag in include_flags
            )
            self.relations[relation_name] = qb.build() + ' GROUP BY block_number, ' + ', '.join(table.columns())

    def get_include_flags(self, table: RTable) -> Iterable[str]:
        for t in self.s_items():
            for rel in t.sources:
                if not isinstance(rel, RTable) and rel.table == table:
                    yield rel.include_flag_name

    def add_sel(self, table: STable) -> None:
        union = []
        r_table = False
        for rel in table.sources:
            if self.is_rel_requested(rel):
                if isinstance(rel, RTable):
                    r_table = True
                    union.append(
                        f'SELECT block_number, {project(table.primary_key_columns())} '
                        f'FROM requested_{rel.request_name()}'
                    )
                elif isinstance(rel, RefRel):
                    projection = ", ".join(
                        c if a == c else f"{c} AS {a}" for a, c in zip(
                            table.primary_key_columns(),
                            rel.key
                        )
                    )
                    union.append(
                        f'SELECT block_number, {projection} '
                        f'FROM requested_{rel.table.request_name()} '
                        f'WHERE include_{rel.include_flag_name}'
                    )
                else:
                    union.append(
                        f'SELECT s.block_number, {project(table.primary_key_columns(), prefix="s.")} '
                        f'FROM {table.table_name()} s, requested_{rel.table.request_name()} r '
                        f'WHERE r.include_{rel.include_flag_name} '
                        f'AND s.block_number = r.block_number '
                        f'AND {rel.join_condition}'
                    )

        if not union:
            return

        self.use_table(table.table_name())

        if len(union) == 1 and r_table:
            select = union[0]
        else:
            select = f'SELECT DISTINCT * FROM ({union_all(union)})'

        self.relations[f'selected_{table.table_name()}'] = select

    def is_rel_requested(self, rel: RTable | RefRel | JoinRel) -> bool:
        if isinstance(rel, RTable):
            return bool(self.q.get(rel.request_name()))

        requests: list[dict] | None
        requests = self.q.get(rel.table.request_name())
        if not requests:
            return False
        for req in requests:
            if req.get(rel.include_flag_name):
                return True
        return False

    def r_items(self) -> Iterable[RTable]:
        for t in self.items():
            if isinstance(t, RTable):
                yield t

    def s_items(self) -> Iterable[STable]:
        for t in self.items():
            if isinstance(t, STable):
                yield t

    def items(self) -> Iterable[RTable | STable]:
        for table in self.model:
            if table.table_name() == 'blocks':
                pass
            else:
                yield table

    def get_blocks_table(self) -> STable:
        for table in self.model:
            if table.table_name() == 'blocks':
                assert isinstance(table, STable)
                return table
        assert False, '"blocks" table must be defined'

    def get_fields(self, table: STable) -> dict:
        return self.q.get('fields', {}).get(table.field_selection_name(), {})

    def add_item_stats(self) -> None:
        sources = []

        for table in self.s_items():
            if f'selected_{table.table_name()}' in self.relations:
                fields = self.get_fields(table)
                weight = table.get_weight(fields)
                sources.append(
                    f'SELECT block_number, {self.new_param(weight)} AS weight '
                    f'FROM selected_{table.table_name()}'
                )

        if sources:
            self.relations['item_stats'] = f'SELECT block_number, SUM(weight) AS weight ' \
                                           f'FROM ({union_all(sources)}) ' \
                                           f'GROUP BY block_number'

    def add_block_stats(self) -> None:
        blocks_table = self.get_blocks_table()
        block_weight = blocks_table.get_weight(self.get_fields(blocks_table))

        if self.q.get('includeAllBlocks'):
            if 'item_stats' in self.relations:
                src = f'SELECT number AS block_number, coalesce(item_stats.weight, 0) AS weight ' \
                      f'FROM blocks ' \
                      f'LEFT OUTER JOIN item_stats ON item_stats.block_number = blocks.number'
            else:
                src = 'SELECT number AS block_number, 0 AS weight FROM blocks'
        elif 'item_stats' in self.relations:
            # ignoring possible duplicates as they don't change much
            src = f'SELECT {self.first_block} AS block_number, 0 AS weight ' \
                  f'UNION ALL ' \
                  f'SELECT * FROM item_stats ' \
                  f'UNION ALL ' \
                  f'SELECT {self.last_block} AS block_number, 0 AS weight'
        else:
            src = f'SELECT {self.first_block} AS block_number, 0 AS weight ' \
                  f'UNION ALL ' \
                  f'SELECT {self.last_block} AS block_number, 0 AS weight'

        self.relations['block_stats'] = f'SELECT ' \
                                        f'block_number, ' \
                                        f'SUM({self.new_param(block_weight)} + weight) ' \
                                        f'OVER ' \
                                        f'(ORDER BY block_number ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) ' \
                                        f'AS size ' \
                                        f'FROM ({src}) ' \
                                        f'WHERE block_number BETWEEN {self.first_block} AND {self.last_block}'

    def add_selected_blocks(self):
        self.relations['selected_blocks'] = f'SELECT * FROM blocks ' \
                                            f'WHERE number IN (' \
                                            f'SELECT block_number FROM block_stats WHERE size <= {self.size_limit} ' \
                                            f'UNION ALL ' \
                                            f'(SELECT block_number FROM block_stats WHERE size > {self.size_limit} ' \
                                            f'ORDER BY block_number LIMIT 1)' \
                                            f') ' \
                                            f'ORDER BY number'

    def build(self) -> str:
        blocks_table = self.get_blocks_table()
        self.use_table('blocks')

        for table in self.r_items():
            self.add_req(table)

        for table in self.s_items():
            self.add_sel(table)

        self.add_item_stats()
        self.add_block_stats()
        self.add_selected_blocks()

        props = [
            ('header', blocks_table.project(self.get_fields(blocks_table)))
        ]

        for table in self.s_items():
            selected = f'selected_{table.table_name()}'
            if selected in self.relations:
                projection = table.project(self.get_fields(table), prefix='t.')
                condition = ' AND '.join(f't.{c} = s.{c}' for c in table.primary_key_columns())
                order = project(table.primary_key_columns(), 't.')
                props.append((
                    table.prop_name(),
                    f'coalesce(('
                    f'SELECT list({projection} ORDER BY {order}) '
                    f'FROM {table.table_name()} AS t, {selected} AS s '
                    f'WHERE t.block_number = s.block_number '
                    f'AND {condition} '
                    f'AND t.block_number = b.number '
                    f'), list_value())'
                ))

        sub_queries = ',\n'.join(
            f'{name} AS ({exp})' for name, exp in self.relations.items()
        )

        return f"""
WITH
{sub_queries}
SELECT {json_project(props)} AS json_data
FROM selected_blocks AS b
        """
