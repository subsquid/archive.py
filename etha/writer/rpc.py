import asyncio
from typing import NamedTuple, Any, Optional

import httpx


class RpcCall(NamedTuple):
    method: str
    params: Optional[list[Any]] = None


class RpcError(Exception):
    def __init__(self, info, call: RpcCall):
        self.info = info
        self.call = call
        self.message = 'rpc error'


class RpcClient:
    def __init__(self, url: str):
        self._id = 0
        self._url = url
        self._client = httpx.AsyncClient(timeout=30_000)

    async def call(self, method: str, params: Optional[list[Any]] = None):
        body = {
            'id': self.id(),
            'jsonrpc': '2.0',
            'method': method,
            'params': params,
        }

        rpc_response = await self._request(body)

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

        rpc_response = await self._request(data)
        assert len(rpc_response) == len(data)

        result: list[Any] = [None] * len(data)
        for res in rpc_response:
            idx = res['id'] - data[0]['id']
            if error := res.get('error'):
                raise RpcError(error, calls[idx])
            result[idx] = res['result']
        return result

    async def _request(self, data):
        backoff = [0.1, 0.5, 2, 5, 10, 20]
        errors = 0
        while True:
            try:
                rpc_response = await self._perform_request(data)
            except httpx.HTTPStatusError as e:
                if errors < len(backoff) and _is_retryable_error(e):
                    timeout = backoff[errors]
                    errors += 1
                    await asyncio.sleep(timeout)
                else:
                    raise e
            else:
                return rpc_response

    async def _perform_request(self, data):
        headers = {
            'accept': 'application/json',
            'accept-encoding': 'gzip, br',
            'content-type': 'application/json',
        }
        response = await self._client.post(self._url, json=data, headers=headers)
        response.raise_for_status()
        return response.json()

    def id(self) -> int:
        id = self._id
        self._id += 1
        return id


def _is_retryable_error(e: httpx.HTTPStatusError) -> bool:
    return e.response.status_code in (429, 502, 503, 504)
