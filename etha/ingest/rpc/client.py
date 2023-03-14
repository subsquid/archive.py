import asyncio
import heapq
import logging
import time
from typing import NamedTuple, Any, Optional

from etha.ingest.rpc.connection import RpcConnection, RpcEndpoint, RpcEndpointMetrics, RpcRequest, RpcRetryException


LOG = logging.getLogger(__name__)


class _ReqItem(NamedTuple):
    priority: int
    id: int
    request: RpcRequest
    future: asyncio.Future


class RpcClient:
    def __init__(self, endpoints: list[RpcEndpoint]):
        self._id = 0
        self._connections = [RpcConnection(e, self._schedule) for e in endpoints]
        self._queue = []
        self._scheduling_soon = False

    def metrics(self) -> list[RpcEndpointMetrics]:
        return [con.metrics() for con in self._connections]

    def call(self, method: str, params: Optional[list[Any]] = None, priority: int = 0) -> asyncio.Future[Any]:
        request = {
            'id': self._next_id(),
            'jsonrpc': '2.0',
            'method': method,
            'params': params
        }

        item = _ReqItem(
            priority,
            request['id'],
            request,
            asyncio.get_event_loop().create_future()
        )

        LOG.debug('rpc call', extra={
            'rpc_call': request['id'],
            'rpc_priority': priority,
            'rpc_method': method,
            'rpc_params': params
        })

        heapq.heappush(self._queue, item)
        self._schedule_soon()
        return item.future

    def _schedule_soon(self):
        if self._scheduling_soon:
            return
        self._scheduling_soon = True
        asyncio.get_event_loop().call_soon(self._schedule)

    def _schedule(self):
        self._scheduling_soon = False
        if not self._queue:
            return

        self._connections.sort(key=lambda c: c.avg_response_time())
        current_time = time.time()

        cit = iter(self._connections)
        while self._queue and (con := next(cit, None)):
            if con.is_online() and (cap := con.get_capacity(current_time)) > 0:
                for item in self._take(cap):
                    asyncio.create_task(self._send(con, item))

        # if random.random() < 0.1:
        #     free = [con
        #             for con in self._connections
        #             if con.is_online() and con.get_capacity(current_time > 0) and con not in plan]
        #     if free:
        #         con = free[random.randrange(0, len(free))]
        #         if self._queue:
        #             item = self._queue.pop(0)
        #         else:
        #             item = next(reversed(plan.values())).pop()
        #         plan[con] = [item]
        #
        # loop = asyncio.get_event_loop()
        #
        # for con, items in plan.items():
        #     for item in items:
        #         loop.create_task(self._send(con, item))

    async def _send(self, con: RpcConnection, item: _ReqItem):
        try:
            result = await con.request(item.request)
            item.future.set_result(result)
        except RpcRetryException:
            self._queue.append(item)
        except Exception as ex:
            item.future.set_exception(ex)
        finally:
            self._schedule_soon()

    def _faster_connections_can_serve(self, con: RpcConnection) -> int:
        response_time = con.avg_response_time()
        count = 0
        for faster in self._connections:
            if faster == con:
                return count
            elif faster.is_online():
                count += faster.can_serve_during(response_time)
        return count

    def _take(self, n: int) -> list[_ReqItem]:
        n = min(len(self._queue), n)
        while n:
            yield heapq.heappop(self._queue)
            n -= 1

    def _next_id(self) -> int:
        next_id = self._id
        self._id += 1
        return next_id
