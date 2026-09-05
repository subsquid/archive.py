import asyncio
import json
import unittest

import httpx

from sqa.util.rpc.client import RpcClient
from sqa.util.rpc.connection import RpcEndpoint


def _mock_client(handler) -> httpx.AsyncClient:
    return httpx.AsyncClient(transport=httpx.MockTransport(handler))


class MethodNotFoundRoutingTest(unittest.IsolatedAsyncioTestCase):
    async def test_method_not_found_routes_to_capable_endpoint(self):
        # First endpoint rejects the trace method with -32601, second serves it.
        # A single endpoint's permanent -32601 must not crash the whole client.
        good_result = {'hash': '0xabc'}

        def bad_handler(request):
            body = json.loads(request.content)
            return httpx.Response(200, json={
                'jsonrpc': '2.0', 'id': body['id'],
                'error': {'code': -32601, 'message': 'method requires a paid plan'}
            })

        def good_handler(request):
            body = json.loads(request.content)
            return httpx.Response(200, json={
                'jsonrpc': '2.0', 'id': body['id'], 'result': good_result
            })

        client = RpcClient([RpcEndpoint('http://bad.example'), RpcEndpoint('http://good.example')])
        bad_con, good_con = client._connections
        bad_con._client = _mock_client(bad_handler)
        good_con._client = _mock_client(good_handler)

        result = await asyncio.wait_for(client.call('debug_traceBlockByHash', ['0x0']), timeout=5)
        client.close()

        self.assertEqual(result, good_result)
        self.assertIn('debug_traceBlockByHash', bad_con.unsupported_methods)
        self.assertNotIn('debug_traceBlockByHash', good_con.unsupported_methods)


if __name__ == '__main__':
    unittest.main()
