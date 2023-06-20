import datetime
import math
from typing import Iterable, Optional, NamedTuple

from etha.fs import LocalFs
from etha.layout import get_chunks
from etha.query.model import Query
from etha.query.runner import QueryRunner
from etha.worker.state.intervals import Range


class QueryResult(NamedTuple):
    result: str
    num_read_chunks: int
    exec_plan: Optional[str] = None


def execute_query(dataset_dir: str, data_range: Range, q: Query, profiling: bool = False) -> QueryResult:
    first_block = max(data_range[0], q['fromBlock'])
    last_block = min(data_range[1], q.get('toBlock', math.inf))
    assert first_block <= last_block

    num_read_chunks = 0
    exec_plans = []

    def json_lines() -> Iterable[str]:
        nonlocal num_read_chunks
        beg = datetime.datetime.now()
        runner = QueryRunner(dataset_dir, q)
        size = 0

        for chunk in get_chunks(LocalFs(dataset_dir), first_block=first_block, last_block=last_block):
            rows, exec_plan = runner.visit(chunk, profiling=profiling)
            if profiling:
                exec_plans.append(exec_plan)
            num_read_chunks += 1

            for row in rows:
                line = row.as_py()
                yield line
                size += len(line)

            if size > 20 * 1024 * 1024:
                break

            if datetime.datetime.now() - beg > datetime.timedelta(seconds=2):
                break

    return QueryResult(
        result=f'[{",".join(json_lines())}]',
        num_read_chunks=num_read_chunks,
        exec_plan=f'[{",".join(exec_plans)}]' if profiling else None
    )
