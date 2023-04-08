import os.path
from typing import Any, TypedDict, NotRequired, Literal, Iterable

from etha.layout import DataChunk
from etha.query.model import Query
from etha.query.util import SqlBuilder, Bin, And, Or, to_snake_case


TableName = Literal['blocks', 'transactions', 'logs']
VariableName = TableName | Literal['last_block']


class VariableMap(TypedDict):
    blocks: int
    transactions: NotRequired[int]
    logs: NotRequired[int]
    last_block: NotRequired[int]


class SqlQuery:
    def __init__(self, q: Query):
        builder = _SqlQueryBuilder(q)
        self.sql = builder.sql
        self.params = builder.params
        self._variables = builder.variables
        self._last_block = q.get('toBlock')

    def set_chunk(self, dataset_dir: str, chunk: DataChunk) -> None:
        table: TableName
        for table in ['blocks', 'transactions', 'logs']:
            if table in self._variables:
                idx = self._variables[table]
                self.params[idx] = os.path.join(dataset_dir, chunk.path(), f'{table}.parquet')

        if 'last_block' in self._variables:
            idx = self._variables['last_block']
            if self._last_block is None:
                self.params[idx] = chunk.last_block
            else:
                self.params[idx] = min(chunk.last_block, self._last_block)


class _SqlQueryBuilder:
    def __init__(self, q: Query, size_limit=1_000_000):
        self.q = q
        self.params = []
        self.variables = {}
        self.relations = {}
        self.size_limit = self.new_param(size_limit)
        self.first_block = self.new_param(self.q.get('fromBlock'))
        self.last_block = self.new_variable('last_block')
        self.sql = self.build()

    def new_param(self, value: Any) -> str:
        self.params.append(value)
        return f'${len(self.params)}'

    def new_variable(self, name: VariableName) -> str:
        assert name not in self.variables
        idx = int(self.new_param(None)[1:])
        self.variables[name] = idx - 1
        return f'${idx}'

    def use_table(self, name: TableName) -> None:
        if name not in self.relations:
            self.relations[name] = f'SELECT * FROM read_parquet({self.new_variable(name)})'

    def add_selected_logs(self) -> None:
        selection = self.q.get('logs')
        if not selection:
            return

        self.use_table('logs')

        qb = SqlBuilder('logs')

        cases = []
        for variant in selection:
            cond = And([])

            where_address = self.in_condition('address', variant.get('address'))
            if where_address:
                cond.ops.append(where_address)

            where_topic = self.in_condition('topic0', variant.get('topic0'))
            if where_topic:
                cond.ops.append(where_topic)

            if cond.ops:
                cases.append(cond)
            else:
                cases = []
                break

        qb.add_where(Or(cases))
        qb.add_columns(['block_number', 'log_index', 'transaction_index'])
        self.relations['selected_logs'] = qb.build()

    def add_requested_transactions(self) -> None:
        selection = self.q.get('transactions', [])
        if not selection:
            return None

        self.use_table('transactions')

        qb = SqlBuilder('transactions')

        cases = []

        for variant in selection:
            cond = And([])

            where_address = self.in_condition('"to"', variant.get('to'))
            if where_address:
                cond.ops.append(where_address)

            where_sighash = self.in_condition('sighash', variant.get('sighash'))
            if where_sighash:
                cond.ops.append(where_sighash)

            if cond.ops:
                cases.append(cond)
            else:
                cases = []
                break

        qb.add_where(Or(cases))
        qb.add_columns(['block_number', 'transaction_index'])
        self.relations['requested_transactions'] = qb.build()

    def add_selected_transactions(self) -> None:
        has_log_txs = 'selected_logs' in self.relations and 'transaction' in self.get_log_fields()
        has_requested_txs = 'requested_transactions' in self.relations
        query = None

        if has_log_txs and has_requested_txs:
            query = 'SELECT DISTINCT block_number, transaction_index FROM (' \
                    'SELECT block_number, transaction_index FROM selected_logs ' \
                    'UNION ALL ' \
                    'SELECT * FROM requested_transactions' \
                    ')'
        elif has_log_txs:
            self.use_table('transactions')
            query = 'SELECT DISTINCT block_number, transaction_index FROM selected_logs'
        elif has_requested_txs:
            query = 'SELECT * FROM requested_transactions'

        if query:
            self.relations['selected_transactions'] = query

    def add_item_stats(self) -> None:
        sources = []

        if 'selected_logs' in self.relations:
            sources.append(
                f'SELECT block_number, {self.new_param(self.get_log_weight())} AS weight '
                f'FROM selected_logs'
            )

        if 'selected_transactions' in self.relations:
            sources.append(
                f'SELECT block_number, {self.new_param(self.get_tx_weight())} AS weight '
                f'FROM selected_transactions'
            )

        if sources:
            self.relations['item_stats'] = f'SELECT block_number, SUM(weight) AS weight ' \
                                           f'FROM ({_union_all(sources)}) ' \
                                           f'GROUP BY block_number'

    def add_block_stats(self) -> None:
        if 'item_stats' not in self.relations:
            return

        if self.q.get('includeAllBlocks'):
            src = f'SELECT number AS block_number, coalesce(item_stats.weight, 0) AS weight ' \
                  f'FROM blocks ' \
                  f'LEFT OUTER JOIN item_stats ON item_stats.block_number = blocks.number'
        else:
            # ignoring possible duplicates as they don't change much
            src = f'SELECT {self.first_block} AS block_number, 0 AS weight ' \
                  f'UNION ALL ' \
                  f'SELECT * FROM item_stats ' \
                  f'UNION ALL ' \
                  f'SELECT {self.last_block} AS block_number, 0 AS weight'

        self.relations['block_stats'] = f'SELECT ' \
                                        f'block_number, ' \
                                        f'SUM({self.new_param(self.get_block_weight())} + weight) ' \
                                        f'OVER ' \
                                        f'(ORDER BY block_number ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) ' \
                                        f'AS size ' \
                                        f'FROM ({src}) ' \
                                        f'WHERE block_number BETWEEN {self.first_block} AND {self.last_block}'

    def add_selected_blocks(self) -> None:
        if 'block_stats' in self.relations:
            query = f'SELECT * FROM blocks ' \
                    f'WHERE number IN (' \
                    f'SELECT block_number FROM block_stats WHERE size <= {self.size_limit}' \
                    f') ' \
                    f'ORDER BY number'
        elif self.q.get('includeAllBlocks'):
            query = f'SELECT * FROM blocks ' \
                    f'WHERE number BETWEEN {self.first_block} AND {self.last_block} ' \
                    f'ORDER BY number ' \
                    f'LIMIT greatest(1, {self.size_limit} / {self.new_param(self.get_block_weight())})'
        else:
            query = f'SELECT * FROM blocks WHERE number IN ({self.first_block}, {self.last_block})'

        self.relations['selected_blocks'] = query

    def build(self) -> str:
        self.use_table('blocks')
        self.add_selected_logs()
        self.add_requested_transactions()
        self.add_selected_transactions()
        self.add_item_stats()
        self.add_block_stats()
        self.add_selected_blocks()

        with_exp = ',\n'.join(
            f'{name} AS ({select})' for name, select in self.relations.items()
        )

        props = [
            ('header', _json_project(self.get_block_fields()))
        ]

        if 'selected_transactions' in self.relations:
            props.append((
                'transactions',
                f'(SELECT json_group_array({_json_project(self.get_tx_fields(), "tx.")}) '
                f'FROM transactions AS tx, selected_transactions AS stx '
                f'WHERE tx.block_number = stx.block_number '
                f'AND tx.transaction_index = stx.transaction_index '
                f'AND tx.block_number = b.number)'
            ))

        if 'selected_logs' in self.relations:
            props.append((
                'logs',
                f'(SELECT json_group_array({self.project_log()}) '
                f'FROM logs, selected_logs AS s '
                f'WHERE logs.block_number = s.block_number '
                f'AND logs.log_index = s.log_index '
                f'AND logs.block_number = b.number)'
            ))

        return f"""
WITH
{with_exp}
SELECT {_json_project(props)} AS json_data 
FROM selected_blocks AS b
        """

    def project_log(self) -> str:
        fields = self.get_log_fields()
        props = []
        for name in fields:
            if name == 'topics':
                props.append(
                    ('topics', '[t for t in list_value(topic0, topic1, topic2, topic3) if t is not null]')
                )
            elif name == 'transaction':
                pass
            else:
                props.append(name)
        return _json_project(props, 'logs.')

    def get_log_weight(self) -> int:
        return _compute_item_weight(self.get_log_fields(), {
            'data': 4,
            'topics': 4
        })

    def get_tx_weight(self) -> int:
        return _compute_item_weight(self.get_tx_fields(), {
            'input': 4
        })

    def get_block_weight(self) -> int:
        return _compute_item_weight(self.get_block_fields(), {
            'logsBloom': 10
        })

    def get_log_fields(self) -> set[str]:
        return self.get_fields('log', {'logIndex', 'transactionIndex'})

    def get_tx_fields(self) -> set[str]:
        return self.get_fields('transaction', {'transactionIndex'})

    def get_block_fields(self) -> set[str]:
        return self.get_fields('block', {'number', 'hash', 'parentHash'})

    def get_fields(self, table: str, required: Iterable[str]) -> set[str]:
        requested = self.q.get('fields', {}).get(table, {})
        fields = set(required)
        fields.update(f for f, included in requested.items() if included)
        return fields

    def in_condition(self, col: str, variants: list | None) -> Bin | None:
        if not variants:
            return None
        elif len(variants) == 1:
            return Bin('=', col, self.new_param(variants[0]))
        else:
            return Bin('IN', col, f"(SELECT UNNEST({self.new_param(variants)}))")


def _json_project(fields: Iterable[str | tuple[str, str]], field_prefix: str = '') -> str:
    props = []
    for alias in fields:
        if isinstance(alias, tuple):
            exp = alias[1]
            alias = alias[0]
        else:
            exp = f'{field_prefix}"{to_snake_case(alias)}"'

        props.append(f"'{alias}'")
        props.append(exp)

    return f'json_object({", ".join(props)})'


def _compute_item_weight(fields: Iterable[str], weights: dict[str, int]) -> int:
    return sum(weights.get(f, 1) for f in fields)


def _union_all(relations: Iterable[str]) -> str:
    return ' UNION ALL '.join(relations)
