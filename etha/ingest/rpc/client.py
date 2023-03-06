import asyncio
from typing import NamedTuple, Any, Optional

import httpx

from etha.ingest.rpc.connection import Connection, RetriableException
from etha.ingest.rpc.generator import connection_generator


class RpcEndpoint(NamedTuple):
    url: str
    limit: Optional[int]


class RpcCall(NamedTuple):
    method: str
    params: Optional[list[Any]] = None


class RpcError(Exception):
    def __init__(self, info, call: RpcCall):
        self.info = info
        self.call = call
        self.message = 'rpc error'


PRIORITY = 1
RETRY_PRIORITY = 0


class _QueueItem(NamedTuple):
    future: asyncio.Future
    data: Any
    priority: int

    def __lt__(self, other):
        if isinstance(other, _QueueItem):
            return self.priority < other.priority


class RpcClient:
    def __init__(self, endpoints: list[RpcEndpoint]):
        client = httpx.AsyncClient(timeout=30_000)
        self._id = 0
        self._connections = [Connection(e.url, e.limit, client) for e in endpoints]
        self._queue = asyncio.PriorityQueue()
        self._control_loop_task = asyncio.create_task(self._control_loop())

    async def call(self, method: str, params: Optional[list[Any]] = None):
        body = {
            'id': self.id(),
            'jsonrpc': '2.0',
            'method': method,
            'params': params,
        }

        rpc_response = await self._schedule_request(body)

        if error := rpc_response.get('error'):
            call = RpcCall(method, params)
            raise RpcError(error, call)
        else:
            return rpc_response['result']

    async def batch(self, calls: list[RpcCall]):
        data = []
        for call in calls:
            data.append({
                'id': self.id(),
                'jsonrpc': '2.0',
                'method': call.method,
                'params': call.params,
            })

        rpc_response = await self._schedule_request(data)
        assert len(rpc_response) == len(data)

        result: list[Any] = [None] * len(data)
        for res in rpc_response:
            idx = res['id'] - data[0]['id']
            if error := res.get('error'):
                raise RpcError(error, calls[idx])
            result[idx] = res['result']
        return result

    def metrics(self):
        return [con.metrics() for con in self._connections]

    def _schedule_request(self, data):
        loop = asyncio.get_event_loop()
        future = loop.create_future()
        item = _QueueItem(future, data, PRIORITY)
        self._queue.put_nowait(item)
        return future

    async def _control_loop(self):
        interval = 0.1
        while True:
            await asyncio.sleep(interval)

            qsize = self._queue.qsize()
            for con in connection_generator(self._connections, interval):
                if qsize == 0:
                    break
                item: _QueueItem = self._queue.get_nowait()
                task = asyncio.create_task(con.request(item.data))
                task.add_done_callback(self._callback(item))
                qsize -= 1

    def _callback(self, item: _QueueItem):
        def inner(task: asyncio.Task):
            if exception := task.exception():
                if isinstance(exception, RetriableException):
                    retry_item = _QueueItem(item.future, item.data, RETRY_PRIORITY)
                    self._queue.put_nowait(retry_item)
                else:
                    item.future.set_exception(exception)
            else:
                item.future.set_result(task.result())
        return inner

    def id(self) -> int:
        id = self._id
        self._id += 1
        return id
