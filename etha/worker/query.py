import datetime
import math
from typing import Iterable

from etha.fs import LocalFs
from etha.layout import get_chunks
from etha.query.model import Query
from etha.query.runner import QueryRunner
from etha.worker.state.intervals import Range


def execute_query(dataset_dir: str, data_range: Range, q: Query) -> str:
    first_block = max(data_range[0], q['fromBlock'])
    last_block = min(data_range[1], q.get('toBlock', math.inf))
    assert first_block <= last_block

    def json_lines() -> Iterable[str]:
        beg = datetime.datetime.now()
        runner = QueryRunner(dataset_dir, q)
        size = 0

        for chunk in get_chunks(LocalFs(dataset_dir), first_block=first_block, last_block=last_block):
            for row in runner.visit(chunk):
                line = row.as_py()
                yield line
                size += len(line)

            if size > 20 * 1024 * 1024:
                break

            if datetime.datetime.now() - beg > datetime.timedelta(seconds=2):
                break

    return f'[{",".join(json_lines())}]'
