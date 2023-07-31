import itertools
from typing import TypedDict, NotRequired, Any, Iterable

from . import SqlQuery
from .model import Table, RefRel, JoinRel
from .util import SelectBuilder, And, Or, union_all, Bin, project, json_list, json_project


class MissingData(Exception):
    pass


class ArchiveQuery(TypedDict):
    fromBlock: int
    toBlock: NotRequired[int]
    includeAllBlocks: NotRequired[bool]
    fields: NotRequired[dict[str, dict[str, bool]]]


def build_sql_query(
        model: list[Table],
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
                 model: list[Table],
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
        if not variants:
            return None
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

    def add_req_rel(self, table: Table) -> None:
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
            qb.add_columns(table.key_columns())
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

        relation_name = f'requested_{table.table_name()}'

        if len(union) == 1:
            self.relations[relation_name] = union[0]
        else:
            qb = SelectBuilder(f'({union_all(union)})')
            qb.add_columns(['block_number'])
            qb.add_columns(table.key_columns())
            qb.add_columns(
                f'MAX(include_{flag}) AS include_{flag}'
                for flag in include_flags
            )
            self.relations[relation_name] = qb.build() + ' GROUP BY ' + ', '.join(table.key())

    def get_include_flags(self, table: Table) -> Iterable[str]:
        for t in self.items():
            for rel in t.relations:
                if rel.table == table:
                    yield rel.include_flag_name

    def add_sel_rel(self, table: Table) -> None:
        union = []
        has_relations = False

        for rel in table.relations:
            if self.is_rel_requested(rel):
                has_relations = True
                if isinstance(rel, RefRel):
                    projection = ", ".join(
                        c if a == c else f"{c} AS {a}" for a, c in zip(
                            table.primary_key_columns(),
                            rel.key
                        )
                    )
                    union.append(
                        f'SELECT block_number, {projection} '
                        f'FROM requested_{rel.table.table_name()} '
                        f'WHERE include_{rel.include_flag_name}'
                    )
                else:
                    union.append(
                        f'SELECT s.block_number, {project(table.primary_key_columns(), prefix="s.")} '
                        f'FROM {table.table_name()} s, requested_{rel.table.table_name()} r '
                        f'WHERE r.include_{rel.include_flag_name} '
                        f'AND s.block_number = r.block_number '
                        f'AND {rel.join_condition}'
                    )

        if f'requested_{table.table_name()}' in self.relations:
            union.append(
                f'SELECT block_number, {project(table.primary_key_columns())} '
                f'FROM requested_{table.table_name()}'
            )

        if not union:
            return

        self.use_table(table.table_name())

        if len(union) == 1 and not has_relations:
            select = union[0]
        else:
            select = f'SELECT DISTINCT * FROM ({union_all(union)})'

        self.relations[f'selected_{table.table_name()}'] = select

    def is_rel_requested(self, rel: RefRel | JoinRel) -> bool:
        requests: list[dict] | None
        requests = self.q.get(rel.table.request_name())
        if not requests:
            return False
        for req in requests:
            if req.get(rel.include_flag_name):
                return True
        return False

    def items(self) -> Iterable[Table]:
        for table in self.model:
            if table.table_name() == 'blocks':
                pass
            else:
                yield table

    def get_blocks_table(self) -> Table:
        for table in self.model:
            if table.table_name() == 'blocks':
                return table
        assert False, '"blocks" table must be defined'

    def get_fields(self, table: Table) -> dict:
        return self.q.get('fields', {}).get(table.selection_name(), {})

    def add_item_stats(self) -> None:
        sources = []

        for table in self.items():
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
                                            f'SELECT block_number FROM block_stats WHERE size <= {self.size_limit}' \
                                            f') ' \
                                            f'ORDER BY number'

    def build(self) -> str:
        blocks_table = self.get_blocks_table()
        self.use_table('blocks')

        for table in self.items():
            self.add_req_rel(table)

        for table in self.items():
            self.add_sel_rel(table)

        self.add_item_stats()
        self.add_block_stats()
        self.add_selected_blocks()

        props = [
            ('header', blocks_table.project(self.get_fields(blocks_table)))
        ]

        for table in self.items():
            selected = f'selected_{table.table_name()}'
            if selected in self.relations:
                projection = table.project(self.get_fields(table), prefix='t.')
                condition = ' AND '.join(f't.{c} = s.{c}' for c in table.primary_key_columns())
                order = project(table.primary_key_columns(), 't.')
                props.append((
                    table.request_name(),
                    json_list(
                        f'SELECT {projection} AS item '
                        f'FROM {table.table_name()} AS t, {selected} AS s '
                        f'WHERE t.block_number = s.block_number '
                        f'AND {condition} '
                        f'AND t.block_number = b.number '
                        f'ORDER BY {order}'
                    )
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
