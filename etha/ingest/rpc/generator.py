import math
import collections
from typing import Generator

from etha.ingest.rpc.connection import Connection


def connection_generator(connections: list[Connection], interval: float) -> Generator[Connection, None, None]:
    '''
    Generator is trying to calculate how many requests can be processed
    within the specified interval per connection
    and spread requests equally between the connections.
    '''
    limits = _calculate_limits(connections, interval)
    state = collections.defaultdict(int)
    while True:
        found_slot = False
        for con in limits:
            taken = state[con]
            if taken < limits[con]:
                state[con] += 1
                found_slot = True
                yield con
        if not found_slot:
            break


def _calculate_limits(connections: list[Connection], interval: float) -> dict[Connection, int]:
    limits = {}
    for connection in connections:
        if not connection.is_online():
            continue
        limit = math.ceil(interval / connection.avg_response_time())
        if connection.limit:
            fixed_limit = math.ceil(connection.limit * interval)
            limit = min(limit, fixed_limit)
        limits[connection] = limit
    return limits
