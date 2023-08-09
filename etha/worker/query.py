import datetime
import math
import time
from typing import Iterable, Optional, NamedTuple

import psutil

from etha.fs import LocalFs
from etha.layout import get_chunks
from etha.query.model import Query
from etha.query.runner import get_query_runner
from etha.worker.state.intervals import Range


class QueryResult(NamedTuple):
    result: str
    num_read_chunks: int
    exec_plan: Optional[str] = None
    exec_time: Optional[dict] = None


def execute_query(dataset_dir: str, data_range: Range, q: Query, profiling: bool = False) -> QueryResult:
    first_block = max(data_range[0], q['fromBlock'])
    last_block = min(data_range[1], q.get('toBlock', math.inf))
    assert first_block <= last_block

    num_read_chunks = 0
    runner = get_query_runner(dataset_dir=dataset_dir, q=q, profiling=profiling)

    def json_lines() -> Iterable[str]:
        nonlocal num_read_chunks
        beg = datetime.datetime.now()
        size = 0

        fs = LocalFs(dataset_dir)
        for chunk in get_chunks(fs, first_block=first_block, last_block=last_block):
            try:
                rows = runner.visit(chunk)
            except Exception as e:
                e.add_note(f'data chunk: ${fs.abs(chunk.path())}')
                raise e

            num_read_chunks += 1

            for row in rows:
                line = row.as_py()
                yield line
                size += len(line)

            if size > 20 * 1024 * 1024:
                break

            if datetime.datetime.now() - beg > datetime.timedelta(seconds=2):
                break

    if profiling:
        ps = psutil.Process()
        beg_times = ps.cpu_times()
        beg = time.time()

    result = f'[{",".join(json_lines())}]'

    if profiling:
        duration = time.time() - beg
        end_times = ps.cpu_times()
        exec_time = {
            'user': end_times.user - beg_times.user,
            'system': end_times.system - beg_times.system,
        }
        try:
            exec_time['iowait'] = end_times.iowait - beg_times.iowait
        except AttributeError:
            pass
        exec_time['elapsed'] = duration
    else:
        exec_time = None

    return QueryResult(
        result=result,
        num_read_chunks=num_read_chunks,
        exec_plan=f'[{",".join(runner.exec_plans)}]' if profiling else None,
        exec_time=exec_time
    )
