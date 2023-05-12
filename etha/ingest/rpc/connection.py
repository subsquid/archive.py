import asyncio
import logging
import sys
import time
from typing import Optional, NamedTuple, Any, TypedDict, Literal, Callable, Union, Tuple, Set

import httpx

from etha.util.counters import Speed, Rate


LOG = logging.getLogger(__name__)


class RpcRequest(TypedDict):
    id: int
    jsonrpc: Literal['2.0']
    method: str
    params: Optional[list[Any]]


BatchRpcRequest = list[RpcRequest]


class RpcError(Exception):
    def __init__(self, info: Any, request: Union[RpcRequest, BatchRpcRequest], url: str):
        self.message = 'rpc error'
        self.info = info
        self.request = request
        self.url = url


class RpcEndpoint(NamedTuple):
    url: str
    capacity: int = 5
    request_timeout: int = 10_000
    rps_limit: Optional[int] = None
    rps_limit_window: int = 10
    missing_methods: Union[list[str], Set[str], Tuple[str, ...]] = ()


class RpcEndpointMetrics(NamedTuple):
    url: str
    avg_response_time: float
    served: int
    errors: int


class RpcRetryException(Exception):
    pass


class _Timer:
    def __init__(self, current_time: Optional[float] = None):
        self.beg = current_time or time.time()
        self._end = None

    @property
    def end(self) -> float:
        assert self._end
        return self._end

    def is_stopped(self) -> bool:
        return self._end is not None

    def stop(self):
        self._end = time.time()

    def time_ms(self) -> int:
        return round((self.end - self.beg) * 1000)


class RpcConnection:
    def __init__(self, endpoint: RpcEndpoint, online_callback: Optional[Callable[[], None]] = None):
        self.endpoint = endpoint
        self._online_callback = online_callback
        self._client = httpx.AsyncClient(
            base_url=endpoint.url,
            timeout=httpx.Timeout(
                connect=5,
                write=5,
                pool=1,
                read=endpoint.request_timeout
            ),
            limits=httpx.Limits(
                max_connections=endpoint.capacity + 1,
                max_keepalive_connections=endpoint.capacity + 1,
                keepalive_expiry=60
            )
        )
        self._speed = Speed(window_size=100)
        self._rate = Rate(window_size=endpoint.rps_limit_window, slot_secs=1/endpoint.rps_limit_window) or None
        self._online = True
        self._backoff_schedule = [10, 100, 500, 2000, 10000, 20000]
        self._served = 0
        self._errors = 0
        self._errors_in_row = 0
        self._pending_requests = 0
        self._extra = {'rpc_url': endpoint.url}
        self.in_queue = 0

    def avg_response_time(self) -> float:
        return self._speed.time() or 0.01

    def metrics(self) -> RpcEndpointMetrics:
        return RpcEndpointMetrics(
            self.endpoint.url,
            self.avg_response_time(),
            self._served,
            self._errors
        )

    def is_online(self) -> bool:
        return self._online

    def get_capacity(self) -> int:
        return max(0, self.endpoint.capacity - self._pending_requests)

    def get_rps_capacity(self, current_time: Optional[float] = None) -> int:
        if self.endpoint.rps_limit:
            return max(0, self.endpoint.rps_limit - self._rate.get(current_time))
        else:
            return sys.maxsize

    def request(
            self,
            req_id: Any,
            request: Union[RpcRequest, BatchRpcRequest],
            current_time: Optional[float] = None
    ) -> asyncio.Task:
        timer = _Timer(current_time=current_time)

        if self._rate:
            size = len(request) if isinstance(request, list) else 1
            self._rate.inc(size, timer.beg)

        self._pending_requests += 1

        return asyncio.create_task(self._handle_request(req_id, request, timer))

    async def _handle_request(self, req_id, request: Union[RpcRequest, BatchRpcRequest], timer: _Timer) -> Any:
        try:
            res = await self._perform_request(req_id, request, timer)
            self._count_request(timer)
            return res
        except Exception as e:
            if _is_retryable_error(e):
                LOG.warning('rpc connection error', exc_info=e, extra={**self._extra, 'rpc_req': req_id})
                self._backoff()
                raise RpcRetryException
            if isinstance(e, RpcError):
                self._count_request(timer)
            raise e
        finally:
            self._pending_requests -= 1

    async def _perform_request(self, req_id: Any, request: Union[RpcRequest, BatchRpcRequest], timer: _Timer) -> Any:
        LOG.debug('rpc send', extra={**self._extra, 'rpc_req': req_id})

        http_response = await self._client.post('/', json=request, headers={
            'accept': 'application/json',
            'accept-encoding': 'gzip, br',
            'content-type': 'application/json',
        })

        timer.stop()

        http_response.raise_for_status()

        result = http_response.json()

        LOG.debug('rpc result', extra={
            **self._extra,
            'rpc_req': req_id,
            'rpc_time': timer.time_ms(),
            'rpc_response': result
        })

        if isinstance(request, list):  # is batch
            if isinstance(result, dict):
                assert 'error' in result
                raise RpcError(result['error'], request, self.endpoint.url)
            else:
                assert isinstance(result, list)
                assert len(result) == len(request)
                result.sort(key=lambda i: i['id'])
                return [self._unpack_result(req, res) for req, res in zip(request, result)]
        else:
            return self._unpack_result(request, result)

    def _unpack_result(self, request: RpcRequest, result) -> Any:
        assert isinstance(result, dict)
        assert request['id'] == result['id']
        if 'error' in result:
            raise RpcError(result['error'], request, self.endpoint.url)
        elif result['result'] is None:
            raise RpcResultIsNull(request, self.endpoint.url)
        else:
            return result['result']

    def _count_request(self, timer: _Timer):
        self._speed.push(1, timer.beg, timer.end)
        self._served += 1
        self._errors_in_row = 0

    def _backoff(self):
        backoff = self._backoff_schedule[min(self._errors_in_row, len(self._backoff_schedule) - 1)]
        LOG.warning(f'going offline for {backoff} ms', extra=self._extra)
        self._errors += 1
        self._errors_in_row += 1
        self._online = False
        asyncio.get_event_loop().call_later(backoff / 1000, self._reconnect)

    def _reconnect(self):
        LOG.debug('online', extra=self._extra)
        self._online = True
        if self._online_callback:
            self._online_callback()


def _is_retryable_error(e: Exception) -> bool:
    if isinstance(e, httpx.HTTPStatusError):
        return e.response.status_code in (429, 502, 503, 504, 530)
    elif isinstance(e, httpx.ConnectError) or isinstance(e, httpx.TimeoutException):
        return True
    elif isinstance(e, httpx.RemoteProtocolError) and 'without sending' in str(e):
        return True
    elif isinstance(e, RpcResultIsNull):
        return True
    elif isinstance(e, RpcError) and isinstance(e.info, dict):
        code = e.info.get('code')
        return code == 429 or code == -32603 or code == -32000
    else:
        return False


class RpcResultIsNull(Exception):
    def __init__(self, request: Union[RpcRequest, BatchRpcRequest], url: str):
        self.message = 'rpc result is null'
        self.request = request
        self.url = url
