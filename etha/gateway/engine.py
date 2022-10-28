import bisect
import math
import os
import re
from functools import cached_property
from typing import Iterable, Optional

import duckdb
import pyarrow

from .query import Query
from .sql import And, Bin, Or, SqlBuilder, SqlQuery

Range = tuple[int, int]


class Engine:
    def __init__(self, data_dir: str):
        self._data_dir = data_dir
        self._con = duckdb.connect(':memory:')

    @cached_property
    def _ranges(self) -> list[Range]:
        ranges = []
        for item in os.listdir(self._data_dir):
            m = re.match(r'^(\d+)-(\d+)$', item)
            if m:
                beg = int(m[1])
                end = int(m[2])
                ranges.append((beg, end))
        ranges.sort()
        return ranges

    def run_query(self, q: Query):
        runner = _QueryRunner(self._con, self._data_dir, q)
        for r in self._get_ranges(q):
            yield runner.run(r)

    def _get_ranges(self, q: Query) -> Iterable[Range]:
        from_block = q.get('fromBlock')
        to_block = q.get('toBlock', math.inf)
        loc = bisect.bisect_left(self._ranges, (from_block, from_block))
        for i in range(loc, len(self._ranges)):
            r = self._ranges[i]
            if to_block < r[0]:
                return
            else:
                yield r


class _QueryRunner:
    def __init__(self, con: duckdb.DuckDBPyConnection, data_dir: str, q: Query):
        self.con = con
        self.data_dir = data_dir
        self.q = q

    def run(self, r: Range):
        blocks = None
        logs = None  # !!! A name of a pyarrow table
        transactions = None  # !!! A name of a pyarrow table

        if self.logs_query:
            self.logs_query.set_file(self.file(r, 'logs.parquet'))
            logs = self.execute(self.logs_query)

        if self.tx_query:
            self.tx_query.set_file(self.file(r, 'transactions.parquet'))
            # !!! A name of a pyarrow table
            log_txs = None
            if self.log_txs_needed:
                log_txs = self.execute(SqlQuery(
                    'SELECT ((block_number::long << 24) + transaction_index::long) AS tx_id FROM logs',
                    []
                ))
            transactions = self.execute(self.tx_query)

        if self.blocks_query:
            self.blocks_query.set_file(self.file(r, 'blocks.parquet'))
            blocks = self.execute(self.blocks_query)

        return blocks, transactions, logs

    def file(self, r: Range, name: str):
        return os.path.join(self.data_dir, f"{r[0]:010d}-{r[1]:010d}", name)

    @cached_property
    def logs_query(self):
        selection = self.q.get('logs')
        if not selection:
            return None

        qb = SqlBuilder()

        self.add_block_range_condition(qb)

        cases = []
        for variant in selection:
            cond = And([])

            where_address = _in_condition(qb, 'address', variant.get('address'))
            if where_address:
                cond.ops.append(where_address)

            where_topic = _in_condition(qb, 'topic0', variant.get('topic0'))
            if where_topic:
                cond.ops.append(where_topic)

            if cond.ops:
                cases.append(cond)
            else:
                cases = []
                break

        qb.add_where(Or(cases))

        qb.add_columns(['block_number', 'log_index', 'transaction_index', ])

        fields = self.q.get('fields', {}).get('log', {})

        if fields.get('topics'):
            qb.add_columns(['topic0', 'topic1', 'topic2', 'topic3'])

        if fields.get('data'):
            qb.add_columns(['data'])

        return qb.build()

    @cached_property
    def tx_query(self):
        selection = self.q.get('transactions', [])

        if not selection and not self.log_txs_needed:
            return None

        qb = SqlBuilder()

        self.add_block_range_condition(qb)

        cases = []

        if self.log_txs_needed:
            cases.append(Bin('IN', '_id', '(SELECT tx_id FROM log_txs)'))

        for variant in selection:
            cond = And([])

            where_address = _in_condition(qb, '"to"', variant.get('to'))
            if where_address:
                cond.ops.append(where_address)

            where_sighash = _in_condition(qb, 'sighash', variant.get('sighash'))
            if where_sighash:
                cond.ops.append(where_sighash)

            if cond.ops:
                cases.append(cond)
            else:
                cases = []
                break

        qb.add_where(Or(cases))

        qb.add_columns(['block_number', 'transaction_index'])
        qb.add_columns(self.q.get('fields', {}).get('transaction'))

        return qb.build()

    @cached_property
    def blocks_query(self) -> Optional[SqlQuery]:
        tables = []

        if self.logs_query:
            tables.append('logs')

        if self.tx_query:
            tables.append('transactions')

        if not tables:
            return None

        union = ' UNION ALL '.join(f'SELECT block_number FROM {t}' for t in tables)

        qb = SqlBuilder()
        qb.add_where(Bin('IN', 'number', f'(SELECT distinct(block_number) FROM ({union}))'))
        qb.add_columns(['number', 'hash'])
        qb.add_columns(self.q.get('fields', {}).get('block'))
        return qb.build()

    @cached_property
    def log_txs_needed(self) -> bool:
        if self.logs_query and self.q.get('fields', {}).get('log', {}).get('transaction'):
            return True
        else:
            return False

    def add_block_range_condition(self, qb: SqlBuilder):
        qb.add_where(Bin('>=', 'block_number', qb.param(self.q.get('fromBlock'))))
        if 'toBlock' in self.q:
            qb.add_where(Bin('<=', 'block_number', qb.param(self.q.get('toBlock'))))

    def execute(self, q: SqlQuery) -> pyarrow.Table:
        self.con.execute(q.sql, parameters=q.params)
        return self.con.fetch_arrow_table()


def _in_condition(qb: SqlBuilder, col: str, variants: Optional[list]) -> Optional[Bin]:
    if not variants:
        return None
    elif len(variants) == 1:
        return Bin('=', col, qb.param(variants[0]))
    else:
        return Bin('IN', col, f"(SELECT UNNEST({qb.param(variants)}))")
