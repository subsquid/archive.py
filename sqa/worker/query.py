import datetime
import math
from typing import Iterable, Optional, NamedTuple

import marshmallow as mm

import sqa.eth.query
from sqa.fs import LocalFs
from sqa.layout import get_chunks, get_filelist
from sqa.query.builder import ArchiveQuery, build_sql_query
from sqa.query.model import Model
from sqa.query.runner import QueryRunner
from sqa.worker.state.intervals import Range


class QueryError(Exception):
    pass


def validate_query(q) -> ArchiveQuery:
    if not isinstance(q, dict):
        raise QueryError('invalid query')

    query_type = q.get('type', 'eth')

    if query_type == 'eth':
        q = _validate_shape(q, sqa.eth.query.QUERY_SCHEMA)
    else:
        raise QueryError(f'invalid query: unknown query type - {query_type}"')

    first_block = q['fromBlock']
    last_block = q.get('toBlock')
    if last_block is not None and last_block < first_block:
        raise QueryError(f'invalid query: fromBlock={last_block} > toBlock={first_block}')

    if _get_query_size(q) > 100:
        raise QueryError('invalid query: too many item requests')

    return q


def _validate_shape(obj, schema: mm.Schema):
    try:
        return schema.load(obj)
    except mm.ValidationError as err:
        raise QueryError(f'invalid query: {err.normalized_messages()}')


def _get_query_size(query: ArchiveQuery) -> int:
    size = 0
    for item in query.values():
        if isinstance(item, list):
            size += len(item)
    return size


def _get_model(q: dict) -> Model:
    query_type = q.get('type', 'eth')
    if query_type == 'eth':
        return sqa.eth.query.MODEL
    else:
        raise TypeError(f'unknown query type - {query_type}')


class QueryResult(NamedTuple):
    result: str
    num_read_chunks: int
    exec_plan: Optional[str] = None


def execute_query(
        dataset_dir: str,
        data_range: Range,
        q: ArchiveQuery,
        profiling: bool = False
) -> QueryResult:
    first_block = max(data_range[0], q['fromBlock'])
    last_block = min(data_range[1], q.get('toBlock', math.inf))
    assert first_block <= last_block

    fs = LocalFs(dataset_dir)

    model = _get_model(q)
    filelist = get_filelist(fs, first_block=first_block)
    sql_query = build_sql_query(model, q, filelist)

    print(sql_query.sql)

    runner = QueryRunner(dataset_dir, sql_query, profiling=profiling)
    num_read_chunks = 0

    def json_lines() -> Iterable[str]:
        nonlocal num_read_chunks
        beg = datetime.datetime.now()
        size = 0

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

    return QueryResult(
        result=f'[{",".join(json_lines())}]',
        num_read_chunks=num_read_chunks,
        exec_plan=f'[{",".join(runner.exec_plans)}]' if profiling else None
    )
