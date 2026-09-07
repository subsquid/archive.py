import unittest

from sqa.util.rpc.connection import RpcConnection, RpcEndpoint, RpcError


def _make_conn() -> RpcConnection:
    return RpcConnection(RpcEndpoint(url='http://rpc.invalid'))


def _rpc_error(code, message='error') -> RpcError:
    request = {'id': 1, 'jsonrpc': '2.0', 'method': 'eth_getTransactionReceipt', 'params': []}
    return RpcError({'code': code, 'message': message}, request, 'http://rpc.invalid')


class RetryableErrorTest(unittest.TestCase):
    def setUp(self):
        self.conn = _make_conn()

    def test_rate_limit_code_is_retryable(self):
        # -32005 "rate-limit exceeded" is transient and must be retried, not crash the ingester
        self.assertTrue(self.conn._is_retryable_error(_rpc_error(-32005, 'rate-limit exceeded')))

    def test_known_transient_codes_still_retryable(self):
        for code in (63, 429, -32000, -32002, -32007, -32017, -32602, -32603):
            self.assertTrue(self.conn._is_retryable_error(_rpc_error(code)), code)

    def test_unknown_rpc_code_not_retryable(self):
        self.assertFalse(self.conn._is_retryable_error(_rpc_error(-32099)))


if __name__ == '__main__':
    unittest.main()
