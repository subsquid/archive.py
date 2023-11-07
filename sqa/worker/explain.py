import json
import os
import sys

import duckdb

from sqa.query.builder import build_sql_query
from .query import _get_model


CON = duckdb.connect(':memory:')


def explain(chunk_dir: str, query_file: str) -> None:
    with open(query_file) as f:
        archive_query = json.load(f)

    sql_query = build_sql_query(
        _get_model(archive_query),
        archive_query,
        os.listdir(chunk_dir)
    )

    sql_query.set_chunk_dir_(chunk_dir)

    CON.execute('EXPLAIN ' + sql_query.sql, sql_query.params)
    for row in CON.fetchall():
        for line in row:
            print(line)


if __name__ == '__main__':
    explain(sys.argv[1], sys.argv[2])
