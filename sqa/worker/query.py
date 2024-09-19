import gzip
import json
import math
import time
from dataclasses import dataclass
from typing import Iterable, Optional

import marshmallow as mm
import psutil

import sqa.eth.query
import sqa.substrate.query
import sqa.solana.query
import sqa.tron.query
import sqa.starknet.query
import sqa.fuel.query
from sqa.fs import LocalFs
from sqa.layout import get_chunks, get_filelist, Partition
from sqa.query.model import Model
from sqa.query.plan import QueryPlan
from sqa.query.schema import ArchiveQuery
from .state.intervals import Range
from .util import sha3_256


class InvalidQuery(Exception):
    pass


def validate_query(q) -> ArchiveQuery:
    if not isinstance(q, dict):
        raise InvalidQuery('query must be a JSON object')

    query_type = q.get('type', 'eth')

    if query_type == 'eth':
        q = _validate_shape(q, sqa.eth.query.QUERY_SCHEMA)
    elif query_type == 'substrate':
        q = _validate_shape(q, sqa.substrate.query.QUERY_SCHEMA)
    elif query_type == 'starknet':
        q = _validate_shape(q, sqa.starknet.query.QUERY_SCHEMA)
    elif query_type == 'solana':
        q = _validate_shape(q, sqa.solana.query.QUERY_SCHEMA)
    elif query_type == 'tron':
        q = _validate_shape(q, sqa.tron.query.QUERY_SCHEMA)
    elif query_type == 'fuel':
        q = _validate_shape(q, sqa.fuel.query.QUERY_SCHEMA)
    else:
        raise InvalidQuery(f'unknown query type - {query_type}"')

    first_block = q['fromBlock']
    last_block = q.get('toBlock')
    if last_block is not None and last_block < first_block:
        raise InvalidQuery(f'fromBlock={last_block} > toBlock={first_block}')

    if _get_query_size(q) > 100:
        raise InvalidQuery('too many item requests')

    return q


def _validate_shape(obj, schema: mm.Schema):
    try:
        return schema.load(obj, unknown=mm.RAISE)
    except mm.ValidationError as err:
        raise InvalidQuery(str(err.normalized_messages()))


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
    elif query_type == 'substrate':
        return sqa.substrate.query.MODEL
    elif query_type == 'starknet':
        return sqa.starknet.query.MODEL
    elif query_type == 'solana':
        return sqa.solana.query.MODEL
    elif query_type == 'tron':
        return sqa.tron.query.MODEL
    elif query_type == 'fuel':
        return sqa.fuel.query.MODEL
    else:
        raise TypeError(f'unknown query type - {query_type}')


@dataclass(frozen=True)
class QueryResult:
    compressed_data: bytes
    data_size: int
    data_sha3_256: Optional[str]
    num_read_chunks: int
    last_block: Optional[int] = None
    exec_time: Optional[dict] = None


_PS = psutil.Process()


def execute_query(
        dataset_dir: str,
        data_range: Range,
        q: ArchiveQuery,
        compute_data_hash: bool,
        profiling: bool
) -> QueryResult:
    first_block = max(data_range[0], q['fromBlock'])
    last_block = min(data_range[1], q.get('toBlock', math.inf))
    assert first_block <= last_block

    beg = time.time()
    if profiling:
        beg_cpu_times = _PS.cpu_times()

    fs = LocalFs(dataset_dir)

    filelist = get_filelist(fs, first_block)

    plan = QueryPlan(
        model=_get_model(q),
        q=q,
        filelist=filelist
    )

    num_read_chunks = 0
    last_visited_block = -1

    def json_lines() -> Iterable[str]:
        nonlocal num_read_chunks
        nonlocal last_visited_block
        size = 0

        for chunk in get_chunks(fs, first_block=first_block, last_block=last_block):
            try:
                rows = plan.fetch(
                    Partition(dataset_dir, chunk)
                ).column('data')
            except Exception as e:
                e.add_note(f'data chunk: ${fs.abs(chunk.path())}')
                raise e

            num_read_chunks += 1
            line = None

            for row in rows:
                line = row.as_py()
                yield line
                size += len(line)

            if line:
                last_visited_block = json.loads(line)['header']['number']

            if size > 20 * 1024 * 1024:
                return

            if time.time() - beg > 2:
                return

            if last_visited_block and last_visited_block < chunk.last_block:
                return

    result = f'[{",".join(json_lines())}]'

    duration = time.time() - beg

    if profiling:
        end_cpu_times = _PS.cpu_times()
        exec_time = {
            'user': end_cpu_times.user - beg_cpu_times.user,
            'system': end_cpu_times.system - beg_cpu_times.system,
        }
        try:
            exec_time['iowait'] = end_cpu_times.iowait - beg_cpu_times.iowait
        except AttributeError:
            pass
    else:
        exec_time = {}

    exec_time['elapsed'] = duration

    data = result.encode()
    compressed_data = gzip.compress(data, mtime=0)

    data_hash = None
    if compute_data_hash:
        data_hash = sha3_256(data)

    return QueryResult(
        compressed_data=compressed_data,
        data_size=len(data),
        data_sha3_256=data_hash,
        num_read_chunks=num_read_chunks,
        last_block=last_visited_block,
        exec_time=exec_time
    )
