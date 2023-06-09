import os.path
from itertools import chain, groupby
from typing import Any, TypedDict, NotRequired, Iterable, Callable, NamedTuple

from etha.layout import DataChunk
from etha.query.model import Query, TxRequest, LogRequest, TraceRequest, StateDiffRequest
from etha.query.util import SqlBuilder, Bin, unique, WhereExp, json_project, \
    compute_item_weight, union_all, And, Or, project, json_list, remove_camel_prefix, to_snake_case


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
        for table in ['blocks', 'transactions', 'logs', 'traces', 'statediffs']:
            if table in self._variables:
                idx = self._variables[table]
                self.params[idx] = os.path.join(dataset_dir, chunk.path(), f'{table}.parquet')

        if 'last_block' in self._variables:
            idx = self._variables['last_block']
            if self._last_block is None:
                self.params[idx] = chunk.last_block
            else:
                self.params[idx] = min(chunk.last_block, self._last_block)


class _Child(NamedTuple):
    table: str
    request_field: str
    key: list[str]


class _Ref(NamedTuple):
    table: str
    request_field: str
    join_condition: str


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

    def new_variable(self, name: str) -> str:
        assert name not in self.variables
        idx = int(self.new_param(None)[1:])
        self.variables[name] = idx - 1
        return f'${idx}'

    def use_table(self, name: str) -> None:
        if name not in self.relations:
            self.relations[name] = f'SELECT * FROM read_parquet({self.new_variable(name)})'

    def add_request_relation(self,
                             table: str,
                             columns: Iterable[str],
                             relations: Iterable[str],
                             where: Callable[[Any], Iterable[WhereExp]],
                             requests_field: str | None = None
                             ):
        requests: list[dict] | None
        requests = self.q.get(requests_field or table)
        if not requests:
            return

        self.use_table(table)

        def relation_mask(req: dict):
            return tuple(map(lambda rel: req.get(rel, False), relations))

        union = []
        requests = requests.copy()
        requests.sort(key=relation_mask)

        for mask, req_group in groupby(requests, key=relation_mask):
            qb = SqlBuilder(table)
            qb.add_columns(columns)
            for i, rel in enumerate(relations):
                qb.add_columns([
                    f'{self.new_param(mask[i])} AS include_{rel}'
                ])

            cases = []
            for req in requests:
                conditions = list(c for c in where(req) if c)
                if conditions:
                    cases.append(And(conditions))
                else:
                    cases = []
                    break
            if cases:
                qb.add_where(Or(cases))

            union.append(qb.build())

        relation_name = f'requested_{table}'

        if len(union) == 1:
            self.relations[relation_name] = union[0]
        else:
            qb = SqlBuilder(f'({union_all(union)})')
            qb.add_columns(columns)
            qb.add_columns(f'MAX(include_{rel}) AS include_{rel}' for rel in relations)
            self.relations[relation_name] = qb.build() + ' GROUP BY ' + ', '.join(columns)

    def is_rel_requested(self, rel: _Child | _Ref) -> bool:
        for req in self.q.get(rel.table, []):
            if req.get(rel.request_field):
                return True
        return False

    def add_select_relation(self,
                            table: str,
                            key: list[str],
                            references: list[_Ref],
                            children: list[_Child]
                            ):

        relation_name = f'selected_{table}'
        union = []

        for child in children:
            if self.is_rel_requested(child):
                projection = ", ".join(
                    c if a == c else f"{c} AS {a}" for a, c in zip(key, child.key)
                )
                union.append(f'SELECT {projection} '
                             f'FROM requested_{child.table} '
                             f'WHERE include_{child.request_field}')

        has_children = bool(union)

        for ref in references:
            if self.is_rel_requested(ref):
                union.append(
                    f'SELECT {project(key, prefix="s.")} '
                    f'FROM {table} s, requested_{ref.table} p '
                    f'WHERE p.include_{ref.request_field} '
                    f'AND s.block_number = p.block_number '
                    f'AND {ref.join_condition}'
                )

        if f'requested_{table}' in self.relations:
            union.append(f'SELECT {project(key)} FROM requested_{table}')

        if not union:
            return

        self.use_table(table)

        if len(union) == 1 and not has_children:
            self.relations[relation_name] = union[0]
        else:
            self.relations[relation_name] = f'SELECT DISTINCT {project(key)} FROM ({union_all(union)})'

    def log_where(self, req: LogRequest) -> Iterable[WhereExp]:
        yield self.in_condition('address', req.get('address'))
        yield self.in_condition('topic0', req.get('topic0'))

    def add_requested_logs(self) -> None:
        self.add_request_relation(
            table='logs',
            columns=['block_number', 'log_index', 'transaction_index'],
            relations=['transaction'],
            where=self.log_where
        )

    def add_selected_logs(self) -> None:
        self.add_select_relation(
            table='logs',
            key=['block_number', 'log_index'],
            references=[
                _Ref(
                    table='transactions',
                    request_field='logs',
                    join_condition='s.block_number = p.block_number AND s.transaction_index = p.transaction_index'
                )
            ],
            children=[]
        )

    def get_log_fields(self) -> list[str]:
        return self.get_fields('log', ['logIndex', 'transactionIndex'])

    def get_log_weight(self) -> int:
        return compute_item_weight(self.get_log_fields(), {
            'data': 4,
            'topics': 4
        })

    def project_log(self, prefix: str) -> str:
        def rewrite_topics(f: str):
            if f == 'topics':
                p = prefix
                return 'topics', f'[t for t in list_value({p}topic0, {p}topic1, {p}topic2, {p}topic3) if t is not null]'
            else:
                return f

        return json_project(
            map(rewrite_topics, self.get_log_fields()),
            prefix
        )

    def trace_where(self, req: TraceRequest) -> Iterable[WhereExp]:
        yield self.in_condition('type', req.get('type'))
        yield self.in_condition('create_from', req.get('createFrom'))
        yield self.in_condition('call_from', req.get('callFrom'))
        yield self.in_condition('call_to', req.get('callTo'))
        yield self.in_condition('call_sighash', req.get('callSighash'))
        yield self.in_condition('suicide_refund_address', req.get('suicideRefundAddress'))
        yield self.in_condition('reward_author', req.get('rewardAuthor'))

    def add_requested_traces(self):
        self.add_request_relation(
            table='traces',
            columns=['block_number', 'transaction_index', 'trace_address'],
            relations=['transaction', 'subtraces', 'parents'],
            where=self.trace_where
        )

    def add_selected_traces(self) -> None:
        self.add_select_relation(
            table='traces',
            key=['block_number', 'transaction_index', 'trace_address'],
            references=[
                _Ref(
                    table='transactions',
                    request_field='traces',
                    join_condition='s.transaction_index = p.transaction_index'
                ),
                _Ref(
                    table='traces',
                    request_field='subtraces',
                    join_condition='s.transaction_index = p.transaction_index AND '
                                   'len(s.trace_address) > len(p.trace_address) AND '
                                   's.trace_address[1:len(p.trace_address)] = p.trace_address'
                ),
                _Ref(
                    table='traces',
                    request_field='parents',
                    join_condition='s.transaction_index = p.transaction_index AND '
                                   'len(s.trace_address) < len(p.trace_address) AND '
                                   's.trace_address = p.trace_address[1:len(s.trace_address)]'
                )
            ],
            children=[]
        )

    def get_trace_fields(self) -> list[str]:
        return self.get_fields('trace', ['transactionIndex', 'traceAddress', 'type'])

    def get_trace_weight(self) -> int:
        return compute_item_weight(self.get_trace_fields(), {})

    def project_trace(self, prefix: str) -> str:
        fields = self.get_trace_fields()

        create_result_fields = [f for f in fields if f.startswith('createResult')]
        create_action_fields = [f for f in fields if f.startswith('create') and not f.startswith('createResult')]
        call_result_fields = [f for f in fields if f.startswith('callResult')]
        call_action_fields = [f for f in fields if f.startswith('call') and not f.startswith('callResult')]
        suicide_fields = [f for f in fields if f.startswith('suicide')]
        reward_fields = [f for f in fields if f.startswith('reward')]

        all_action_fields = set(chain(create_action_fields, call_action_fields, suicide_fields, reward_fields))
        rest_fields = [f for f in fields if f not in all_action_fields]

        topics = []

        if create_action_fields or create_result_fields:
            topics.append(('create', create_action_fields, create_result_fields))

        if call_action_fields or call_result_fields:
            topics.append(('call', call_action_fields, call_result_fields))

        if suicide_fields:
            topics.append(('suicide', suicide_fields, []))

        if reward_fields:
            topics.append(('reward', reward_fields, []))

        if topics:
            cases = []

            def topic_projection(_topic: str, _topic_fields: list[str]) -> str:
                return json_project(
                    (remove_camel_prefix(f, _topic), f'{prefix}{to_snake_case(f)}')
                    for f in _topic_fields
                )

            for topic, action_fields, result_fields in topics:
                ext = []

                if action_fields:
                    ext.append(('action', topic_projection(topic, action_fields)))

                if result_fields:
                    ext.append(('result', topic_projection(f'{topic}Result', result_fields)))

                proj = chain(rest_fields, ext)

                cases.append(
                    f"WHEN {prefix}type='{topic}' THEN {json_project(proj, prefix)}"
                )

            when_exps = ' '.join(cases)
            return f'CASE {when_exps} ELSE {json_project(rest_fields, prefix)} END'
        else:
            return json_project(rest_fields, prefix)

    def statediff_where(self, req: StateDiffRequest) -> Iterable[WhereExp]:
        yield self.in_condition('address', req.get('address'))
        yield self.in_condition('key', req.get('key'))
        yield self.in_condition('kind', req.get('kind'))

    def add_requested_statediffs(self):
        self.add_request_relation(
            table='statediffs',
            requests_field='stateDiffs',
            columns=['block_number', 'transaction_index', 'address', 'key'],
            relations=['transaction'],
            where=self.statediff_where
        )

    def add_selected_statediffs(self):
        self.add_select_relation(
            table='statediffs',
            key=['block_number', 'transaction_index', 'address', 'key'],
            references=[
                _Ref(
                    table='transactions',
                    request_field='stateDiffs',
                    join_condition='s.transaction_index = p.transaction_index'
                )
            ],
            children=[]
        )

    def get_statediff_weight(self) -> int:
        return compute_item_weight(self.get_statediff_fields(), {})

    def get_statediff_fields(self) -> list[str]:
        return self.get_fields('stateDiff', ['transactionIndex', 'kind'])

    def project_statediff(self, prefix: str):
        return json_project(self.get_statediff_fields(), prefix)

    def tx_where(self, req: TxRequest):
        yield self.in_condition('"to"', req.get('to'))
        yield self.in_condition('"from"', req.get('from'))
        yield self.in_condition('sighash', req.get('sighash'))

    def add_requested_transactions(self) -> None:
        self.add_request_relation(
            table='transactions',
            columns=['block_number', 'transaction_index'],
            relations=['traces', 'logs', 'stateDiffs'],
            where=self.tx_where
        )

    def add_selected_transactions(self) -> None:
        self.add_select_relation(
            table='transactions',
            key=['block_number', 'transaction_index'],
            references=[],
            children=[
                _Child(
                    table='logs',
                    request_field='transaction',
                    key=['block_number', 'transaction_index']
                ),
                _Child(
                    table='traces',
                    request_field='transaction',
                    key=['block_number', 'transaction_index']
                ),
                _Child(
                    table='statediffs',
                    request_field='transaction',
                    key=['block_number', 'transaction_index']
                )
            ]
        )

    def get_tx_fields(self) -> list[str]:
        return self.get_fields('transaction', ['transactionIndex'])

    def get_tx_weight(self) -> int:
        return compute_item_weight(self.get_tx_fields(), {
            'input': 4
        })

    def project_tx(self, prefix: str) -> str:
        return json_project(self.get_tx_fields(), prefix)

    def add_item_stats(self) -> None:
        sources = []

        self.append_weight_source(sources, 'transactions', self.get_tx_weight())
        self.append_weight_source(sources, 'logs', self.get_log_weight())
        self.append_weight_source(sources, 'traces', self.get_trace_weight())
        self.append_weight_source(sources, 'statediffs', self.get_statediff_weight())

        if sources:
            self.relations['item_stats'] = f'SELECT block_number, SUM(weight) AS weight ' \
                                           f'FROM ({union_all(sources)}) ' \
                                           f'GROUP BY block_number'

    def append_weight_source(self, sources: list[str], table: str, weight: int) -> None:
        if f'selected_{table}' in self.relations:
            sources.append(
                f'SELECT block_number, {self.new_param(weight)} AS weight '
                f'FROM selected_{table}'
            )

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
        self.add_requested_transactions()
        self.add_requested_logs()
        self.add_requested_traces()
        self.add_requested_statediffs()
        self.add_selected_transactions()
        self.add_selected_logs()
        self.add_selected_traces()
        self.add_selected_statediffs()
        self.add_item_stats()
        self.add_block_stats()
        self.add_selected_blocks()

        with_exp = ',\n'.join(
            f'{name} AS ({select})' for name, select in self.relations.items()
        )

        props = [
            ('header', self.project_block())
        ]

        if 'selected_transactions' in self.relations:
            props.append((
                'transactions',
                json_list(
                    f'SELECT {self.project_tx("tx.")} AS obj'
                    f' FROM transactions AS tx, selected_transactions AS stx'
                    f' WHERE tx.block_number = stx.block_number'
                    f' AND tx.transaction_index = stx.transaction_index'
                    f' AND tx.block_number = b.number'
                    f' ORDER BY tx.transaction_index'
                )
            ))

        if 'selected_logs' in self.relations:
            props.append((
                'logs',
                json_list(
                    f'SELECT {self.project_log("logs.")} AS obj'
                    f' FROM logs, selected_logs AS s'
                    f' WHERE logs.block_number = s.block_number'
                    f' AND logs.log_index = s.log_index'
                    f' AND logs.block_number = b.number'
                    f' ORDER BY logs.log_index'
                )
            ))

        if 'selected_traces' in self.relations:
            props.append((
                'traces',
                json_list(
                    f'SELECT {self.project_trace("tr.")} AS obj'
                    f' FROM traces AS tr, selected_traces AS s'
                    f' WHERE tr.block_number = s.block_number'
                    f' AND tr.transaction_index = s.transaction_index'
                    f' AND tr.trace_address = s.trace_address'
                    f' AND tr.block_number = b.number'
                    f' ORDER BY tr.transaction_index, tr.trace_address'
                )
            ))

        if 'selected_statediffs' in self.relations:
            props.append((
                'stateDiffs',
                json_list(
                    f'SELECT {self.project_statediff("diff.")} AS obj'
                    f' FROM statediffs AS diff, selected_statediffs AS s'
                    f' WHERE diff.block_number = s.block_number'
                    f' AND diff.transaction_index = s.transaction_index'
                    f' AND diff.address = s.address'
                    f' AND diff.key = s.key'
                    f' AND diff.block_number = b.number'
                    f' ORDER BY diff.transaction_index, diff.address, diff.key'
                )
            ))

        return f"""
WITH
{with_exp}
SELECT {json_project(props)} AS json_data 
FROM selected_blocks AS b
        """

    def get_block_fields(self) -> list[str]:
        return self.get_fields('block', ['number', 'hash', 'parentHash'])

    def get_block_weight(self) -> int:
        return compute_item_weight(self.get_block_fields(), {
            'logsBloom': 10
        })

    def project_block(self) -> str:
        def rewrite_timestamp(f: str):
            if f == 'timestamp':
                return 'timestamp', 'epoch(timestamp)'
            else:
                return f

        return json_project(
            map(rewrite_timestamp, self.get_block_fields())
        )

    def get_fields(self, table: str, required: Iterable[str]) -> list[str]:
        requested = self.q.get('fields', {}).get(table, {})
        return list(
            unique(
                chain(required, requested)
            )
        )

    def in_condition(self, col: str, variants: list | None) -> Bin | None:
        if not variants:
            return None
        elif len(variants) == 1:
            return Bin('=', col, self.new_param(variants[0]))
        else:
            return Bin('IN', col, f"(SELECT UNNEST({self.new_param(variants)}))")
