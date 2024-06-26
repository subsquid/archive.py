import asyncio
import heapq
import logging
import sys
import time
from dataclasses import dataclass
from functools import cached_property
from typing import Any, Optional, Union, Set, TypeVar, Iterable, Callable

from sqa.util.rpc.connection import RpcConnection, RpcEndpoint, RpcEndpointMetrics, RpcRequest, RpcRetryException


LOG = logging.getLogger(__name__)


RpcBatchCallItem = tuple[str, Optional[list[Any]]]
RpcBatchCall = list[RpcBatchCallItem]


@dataclass(order=True)
class _ReqItem:
    priority: int
    id: int
    request: Union[RpcRequest, list[RpcRequest]]
    validate_result: Callable[[Any], bool]
    future: asyncio.Future

    @cached_property
    def methods(self) -> Set[str]:
        if isinstance(self.request, list):
            return set(req['method'] for req in self.request)
        else:
            return {self.request['method']}

    def size(self) -> int:
        if isinstance(self.request, list):
            return len(self.request)
        else:
            return 1


class RpcClient:
    _queue: list[_ReqItem]
    _back_queue: list[_ReqItem]
    _scheduling_timer: Optional[asyncio.TimerHandle]

    def __init__(self, endpoints: list[RpcEndpoint], batch_limit: int = 200):
        assert endpoints
        self.batch_limit = batch_limit
        self._connections = [RpcConnection(e, self._schedule) for e in endpoints]
        self._id = 0
        self._queue = []
        self._back_queue = []
        self._scheduling_soon = False
        self._scheduling_timer = None
        self._closed = False

    def metrics(self) -> list[RpcEndpointMetrics]:
        return [con.metrics() for con in self._connections]

    def get_total_capacity(self) -> int:
        total = 0
        for c in self._connections:
            if c.is_online():
                total += c.endpoint.capacity
        return total

    def call(
            self,
            method: str,
            params: Optional[list[Any]] = None,
            priority: int = 0,
            validate_result: Callable[[Any], bool] = lambda _: True
        ) -> asyncio.Future[Any]:
        req_id = self._id
        self._id += 1

        request = {
            'id': req_id,
            'jsonrpc': '2.0',
            'method': method,
            'params': params
        }

        item = _ReqItem(
            priority,
            req_id,
            request,
            validate_result,
            asyncio.get_event_loop().create_future()
        )

        LOG.debug('rpc call', extra={
            'rpc_req': req_id,
            'rpc_call_id': req_id,
            'rpc_priority': priority,
            'rpc_method': method,
            'rpc_params': params
        })

        self._push(item)
        self._schedule_soon()
        return item.future

    def batch_call(
            self,
            calls: list[RpcBatchCallItem],
            priority: int = 0,
            validate_result: Callable[[Any], bool] = lambda _: True
        ) -> asyncio.Future[list[Any]]:
        if not calls:
            f = asyncio.get_event_loop().create_future()
            f.set_result([])
            return f

        futures: list[asyncio.Future[list[Any]]] = []

        for batch in _split_list(calls, self._max_batch_size):
            request = []
            req_id = self._id

            for i, (method, params) in enumerate(batch):
                call_id = self._id
                self._id += 1

                request.append({
                    'id': call_id,
                    'jsonrpc': '2.0',
                    'method': method,
                    'params': params
                })

                LOG.debug('rpc call', extra={
                    'rpc_req': req_id,
                    'rpc_call_id': call_id,
                    'rpc_priority': priority,
                    'rpc_method': method,
                    'rpc_params': params
                })

            item = _ReqItem(
                priority,
                req_id,
                request,
                validate_result,
                asyncio.get_event_loop().create_future()
            )

            futures.append(item.future)
            self._push(item)

        self._schedule_soon()

        return _combine_list_futures(futures)

    def close(self):
        self._closed = True

    @cached_property
    def _max_batch_size(self) -> int:
        min_rps = min((c.endpoint.rps_limit or sys.maxsize) for c in self._connections)
        min_rps_batch = max(1, round(min_rps / 5))
        return min(min_rps_batch, self.batch_limit)

    def _schedule_soon(self):
        if self._scheduling_soon:
            return
        self._scheduling_soon = True
        asyncio.get_event_loop().call_soon(self._schedule)

    def _schedule_later(self):
        if self._scheduling_timer:
            return

        def callback():
            self._scheduling_timer = None
            self._schedule()

        self._scheduling_timer = asyncio.get_event_loop().call_later(0.12, callback)

    def _schedule(self):
        self._scheduling_soon = False
        if not self._queue:
            return

        current_time = time.time()
        schedule_later = False

        connections = []
        for c in self._connections:
            if c.is_online() and c.get_capacity() > 0:
                rpc_cap = c.get_rps_capacity(current_time)
                if rpc_cap > 0:
                    connections.append(c)
                else:
                    schedule_later = True

        put_back: list[_ReqItem] = []

        while connections and (item := self._pop()):
            connections.sort(key=lambda c: (c.in_queue, c.avg_response_time()))
            con_idx = None
            rpc_limit_hit = False
            for i, c in enumerate(connections):
                if not self._can_handle(c, item):
                    continue
                rps_cap = c.get_rps_capacity(current_time)
                if rps_cap < item.size():
                    rpc_limit_hit = True
                    continue
                con_idx = i
                break

            if con_idx is None:
                schedule_later = schedule_later or rpc_limit_hit
                put_back.append(item)
                self._reg_in_queue(item)
            else:
                c = connections[con_idx]
                self._send(c, item, current_time)
                if c.get_capacity() == 0 or c.get_rps_capacity(current_time) == 0:
                    del connections[con_idx]

        for item in put_back:
            heapq.heappush(self._queue, item)

        if schedule_later:
            self._schedule_later()
        elif self._scheduling_timer:
            self._scheduling_timer.cancel()
            self._scheduling_timer = None

    def _push(self, item: _ReqItem):
        self._reg_in_queue(item)
        heapq.heappush(self._queue, item)

    def _reg_in_queue(self, item: _ReqItem):
        has_handlers = False
        for c in self._connections:
            if self._can_handle(c, item):
                has_handlers = True
                c.in_queue += 1
        assert has_handlers

    def _pop(self) -> Optional[_ReqItem]:
        try:
            item = heapq.heappop(self._queue)
        except IndexError:
            return None
        for c in self._connections:
            if self._can_handle(c, item):
                c.in_queue -= 1
        return item

    def _can_handle(self, c: RpcConnection, item: _ReqItem) -> bool:
        if c.endpoint.missing_methods:
            return all(m not in item.methods for m in c.endpoint.missing_methods)
        else:
            return True

    def _send(self, con: RpcConnection, item: _ReqItem, current_time: float):
        def callback(fut):
            try:
                ex = fut.exception()
            except asyncio.CancelledError as e:
                if not self._closed:
                    raise e
            else:
                if not item.future.cancelled():
                    if ex is None:
                        result = fut.result()
                        item.future.set_result(result)
                    elif isinstance(ex, RpcRetryException):
                        self._push(item)
                    else:
                        item.future.set_exception(ex)
                self._schedule_soon()

        future = con.request(item.id, item.request, item.validate_result, current_time=current_time)
        future.add_done_callback(callback)


_T = TypeVar('_T')


def _split_list(ls: list[_T], max_size: int) -> Iterable[list[_T]]:
    assert max_size > 0

    if len(ls) <= max_size:
        yield ls
        return

    pos = 0
    while len(ls) - pos > 2 * max_size:
        yield ls[pos:pos + max_size]
        pos += max_size

    s = (len(ls) - pos) // 2
    yield ls[pos:pos + s]
    yield ls[pos + s:]


def _combine_list_futures(futures: list[asyncio.Future[list[_T]]]) -> asyncio.Future[list[_T]]:
    assert futures

    if len(futures) == 1:
        return futures[0]

    result_fut = asyncio.get_event_loop().create_future()

    batches: list[Any] = [None for _ in range(0, len(futures))]
    left = len(batches)

    def make_done_callback(idx: int):
        def callback(fut: asyncio.Future):
            if result_fut.done():
                return
            if fut.exception():
                result_fut.set_exception(fut.exception())
            else:
                batches[idx] = fut.result()
                nonlocal left
                left -= 1
                if left == 0:
                    result_fut.set_result(
                        [item for batch in batches for item in batch]
                    )
        return callback

    for i, f in enumerate(futures):
        f.add_done_callback(make_done_callback(i))

    return result_fut
