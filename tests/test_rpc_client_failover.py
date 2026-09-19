import asyncio
import json
import unittest

import httpx

from sqa.util.rpc.client import RpcClient
from sqa.util.rpc.connection import RpcEndpoint, MAX_ERRORS_IN_ROW


def _mock_client(handler) -> httpx.AsyncClient:
    return httpx.AsyncClient(transport=httpx.MockTransport(handler))


def _forbidden_handler(request):
    # a dead / revoked key answers every method with HTTP 403 (e.g. Ankr -32052)
    body = json.loads(request.content)
    return httpx.Response(403, json={
        'jsonrpc': '2.0', 'id': body['id'],
        'error': {'code': -32052, 'message': 'API key is not allowed to access blockchain'}
    })


def _ok_handler(result):
    def handler(request):
        body = json.loads(request.content)
        return httpx.Response(200, json={'jsonrpc': '2.0', 'id': body['id'], 'result': result})
    return handler


class PersistentEndpointFailureRoutingTest(unittest.IsolatedAsyncioTestCase):
    async def test_dead_endpoint_routes_to_healthy_one_instead_of_crashing(self):
        # bad endpoint returns a persistent, non-retryable HTTP 403; good endpoint serves.
        # Once bad has failed past the threshold its error is fatal in _is_retryable_error;
        # a single dead endpoint must NOT crash the whole client while a healthy sibling exists.
        client = RpcClient([RpcEndpoint('http://bad.example'), RpcEndpoint('http://good.example')])
        bad_con, good_con = client._connections
        bad_con._client = _mock_client(_forbidden_handler)
        good_con._client = _mock_client(_ok_handler({'hash': '0xabc'}))
        # simulate the persistently-failed state reached in production (avoids 20 real back-offs)
        bad_con._errors_in_row = MAX_ERRORS_IN_ROW + 1

        result = await asyncio.wait_for(client.call('eth_getBlockByNumber', ['0x1', False]), timeout=5)
        client.close()

        self.assertEqual(result, {'hash': '0xabc'})
        self.assertFalse(bad_con.is_healthy())
        self.assertFalse(bad_con.is_online())  # taken out of rotation

    async def test_error_still_surfaces_when_no_healthy_endpoint(self):
        # safety valve: with no other endpoint that can serve it, a non-retryable error
        # must still propagate (surface the problem) rather than spin forever.
        client = RpcClient([RpcEndpoint('http://bad.example')])
        bad_con, = client._connections
        bad_con._client = _mock_client(_forbidden_handler)
        bad_con._errors_in_row = MAX_ERRORS_IN_ROW + 1

        with self.assertRaises(httpx.HTTPStatusError):
            await asyncio.wait_for(client.call('eth_getBlockByNumber', ['0x1', False]), timeout=5)
        client.close()


if __name__ == '__main__':
    unittest.main()
