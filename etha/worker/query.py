import datetime

import duckdb

from .intervals import Range
from ..fs import LocalFs
from ..layout import get_chunks
from ..query.engine import QueryRunner
from ..query.model import Query

CON = duckdb.connect(':memory:')


def execute_query(data_dir: str, data_range: Range, q: Query):
    first_block = max(data_range[0], q['fromBlock'])
    last_block = q.get('toBlock')
    if last_block is None:
        last_block = data_range[1]
    else:
        last_block = min(data_range[1], last_block)

    assert first_block <= last_block

    runner = QueryRunner(CON, data_dir, q)
    fs = LocalFs(data_dir)

    blocks_count = 0
    tx_count = 0
    logs_count = 0
    last_processed_block = None

    beg = datetime.datetime.now()
    for chunk in get_chunks(fs, first_block=first_block, last_block=last_block):
        blocks, txs, logs = runner.run(chunk)
        if blocks:
            blocks_count += blocks.shape[0]
        if txs:
            tx_count += txs.shape[0]
        if logs:
            logs_count += logs.shape[0]

        last_processed_block = min(chunk.last_block, last_block)

        now = datetime.datetime.now()
        if now - beg > datetime.timedelta(seconds=2):
            break

    assert last_processed_block is not None

    return {
        'blocks': blocks_count,
        'txs': tx_count,
        'logs': logs_count,
        'last_block': last_processed_block
    }
