import duckdb
import pyarrow


CON = duckdb.connect(':memory:')


def execute_sql(sql: str, params: list | None = None) -> pyarrow.Table:
    # print(sql)
    CON.execute(sql, params)
    return CON.fetch_arrow_table()
