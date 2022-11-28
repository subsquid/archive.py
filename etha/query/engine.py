import math
import os.path
from functools import cached_property
from typing import Iterable, Optional

import duckdb
import pyarrow

from .model import Query
from .sql import And, Bin, Or, SqlBuilder, SqlQuery
from ..fs import LocalFs
from ..layout import DataChunk, get_chunks


class Engine:
    def __init__(self, data_dir: str):
        self._data_dir = data_dir
        self._con = duckdb.connect(':memory:')

    def run_query(self, q: Query):
        runner = QueryRunner(self._con, self._data_dir, q)
        for r in self._get_chunks(q):
            yield runner.run(r)

    def _get_chunks(self, q: Query) -> Iterable[DataChunk]:
        first_block = q.get('fromBlock')
        last_block = q.get('toBlock', math.inf)
        return get_chunks(LocalFs(self._data_dir), first_block=first_block, last_block=last_block)


class QueryRunner:
    def __init__(self, con: duckdb.DuckDBPyConnection, data_dir: str, q: Query):
        self.con = con
        self.data_dir = data_dir
        self.q = q

    def run(self, chunk: DataChunk) -> tuple[Optional[pyarrow.Table], Optional[pyarrow.Table], Optional[pyarrow.Table]]:
        blocks = None
        logs = None  # !!! A name of a pyarrow table
        transactions = None  # !!! A name of a pyarrow table

        if self._logs_query:
            self._logs_query.set_file(self._file(chunk, 'logs.parquet'))
            logs = self._execute(self._logs_query)

        if self._tx_query:
            self._tx_query.set_file(self._file(chunk, 'transactions.parquet'))
            # !!! A name of a pyarrow table
            log_txs = None
            if self._log_txs_needed:
                log_txs = self._execute(SqlQuery(
                    'SELECT ((block_number::long << 24) + transaction_index::long) AS tx_id FROM logs',
                    []
                ))
            transactions = self._execute(self._tx_query)

        if self._blocks_query:
            self._blocks_query.set_file(self._file(chunk, 'blocks.parquet'))
            blocks = self._execute(self._blocks_query)

        return blocks, transactions, logs

    def _file(self, chunk: DataChunk, name: str):
        return os.path.join(self.data_dir, chunk.path(), name)

    @cached_property
    def _logs_query(self):
        selection = self.q.get('logs')
        if not selection:
            return None

        qb = SqlBuilder()

        self._add_block_range_condition(qb)

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
    def _tx_query(self):
        selection = self.q.get('transactions', [])

        if not selection and not self._log_txs_needed:
            return None

        qb = SqlBuilder()

        self._add_block_range_condition(qb)

        cases = []

        if self._log_txs_needed:
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
    def _blocks_query(self) -> Optional[SqlQuery]:
        tables = []

        if self._logs_query:
            tables.append('logs')

        if self._tx_query:
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
    def _log_txs_needed(self) -> bool:
        if self._logs_query and self.q.get('fields', {}).get('log', {}).get('transaction'):
            return True
        else:
            return False

    def _add_block_range_condition(self, qb: SqlBuilder):
        qb.add_where(Bin('>=', 'block_number', qb.param(self.q.get('fromBlock'))))
        if 'toBlock' in self.q:
            qb.add_where(Bin('<=', 'block_number', qb.param(self.q.get('toBlock'))))

    def _execute(self, q: SqlQuery) -> pyarrow.Table:
        self.con.execute(q.sql, parameters=q.params)
        return self.con.fetch_arrow_table()


def _in_condition(qb: SqlBuilder, col: str, variants: Optional[list]) -> Optional[Bin]:
    if not variants:
        return None
    elif len(variants) == 1:
        return Bin('=', col, qb.param(variants[0]))
    else:
        return Bin('IN', col, f"(SELECT UNNEST({qb.param(variants)}))")
