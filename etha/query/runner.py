import os.path
from typing import Optional

import duckdb
import pyarrow

from etha.query.model import Query
from etha.query.result_set import ResultSet
from etha.query.sql import And, Bin, Or, SqlBuilder, SqlQuery

CON = duckdb.connect(':memory:')


class QueryRunner:
    def __init__(self, q: Query):
        self._runner = _Runner(CON, q)

    def visit(self, chunk_dir: str, rs: ResultSet) -> None:
        blocks, txs, logs = self._runner.visit(chunk_dir)
        rs.write_blocks(blocks)
        rs.write_transactions(txs)
        rs.write_logs(logs)


class _Runner:
    def __init__(self, con: duckdb.DuckDBPyConnection, q: Query):
        self.con = con
        self.q = q
        self.logs_query = self.build_logs_query()
        self.fetch_log_txs = self.logs_query and self.q.get('fields', {}).get('log', {}).get('transaction')
        self.tx_query = self.build_tx_query()
        self.blocks_query = self.build_logs_query()

    def visit(self, chunk_dir: str) -> tuple[Optional[pyarrow.Table], Optional[pyarrow.Table], Optional[pyarrow.Table]]:
        blocks = None
        logs = None  # !!! A name of a pyarrow table
        transactions = None  # !!! A name of a pyarrow table

        if self.logs_query:
            self.logs_query.set_file(os.path.join(chunk_dir, 'logs.parquet'))
            logs = self.execute(self.logs_query)

        if self.tx_query:
            self.tx_query.set_file(os.path.join(chunk_dir, 'transactions.parquet'))
            # !!! A name of a pyarrow table
            log_txs = None
            if self.fetch_log_txs:
                log_txs = self.execute(SqlQuery(
                    'SELECT ((block_number::long << 24) + transaction_index::long) AS tx_id FROM logs',
                    []
                ))
            transactions = self.execute(self.tx_query)

        if self.blocks_query:
            self.blocks_query.set_file(os.path.join(chunk_dir, 'blocks.parquet'))
            blocks = self.execute(self.blocks_query)

        return blocks, transactions, logs

    def build_logs_query(self):
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

    def build_tx_query(self):
        selection = self.q.get('transactions', [])

        if not (selection or self.fetch_log_txs):
            return None

        qb = SqlBuilder()

        self.add_block_range_condition(qb)

        cases = []

        if self.fetch_log_txs:
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

    def build_blocks_query(self) -> Optional[SqlQuery]:
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
