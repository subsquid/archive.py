import duckdb
import pyarrow

from etha.layout import DataChunk, get_chunks
from etha.query.model import Query
from etha.query.sql import SqlQuery


CON = duckdb.connect(':memory:')


class QueryRunner:
    def __init__(self, dataset_dir: str, q: Query):
        self._dataset_dir = dataset_dir
        self._query = SqlQuery(q)

    def visit(self, chunk: DataChunk) -> pyarrow.ChunkedArray:
        self._query.set_chunk(self._dataset_dir, chunk)
        CON.execute(self._query.sql, self._query.params)
        return CON.fetch_arrow_table().column(0)


def perform_test():
    dataset_dir = 'data/worker/czM6Ly9ldGhhLW1haW5uZXQtc2lh'
    runner = QueryRunner(dataset_dir, {
        'fromBlock': 0,
        'fields': {
            'block': {
                'number': True,
                'hash': True,
                'parentHash': True,
            },
            'log': {
                'topics': True,
                'data': True,
                'transaction': True
            },
            'transaction': {
                'hash': True,
                'from': True
            }
        },
        'logs': [
            {
                'address': ['0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'],
                'topic0': ['0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef']
            }
        ],
        # 'transactions': [
        #     {
        #         'to': ['0x3883f5e181fccaf8410fa61e12b59bad963fb645']
        #     }
        # ]
    })

    from etha.fs import LocalFs
    size = 0
    for chunk in get_chunks(LocalFs(dataset_dir)):
        for row in runner.visit(chunk):
            line = row.as_py()
            print(line)
            size += len(line)
        if size > 20 * 1024 * 1024:
            return


if __name__ == '__main__':
    perform_test()
