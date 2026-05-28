import time
import unittest
from dataclasses import dataclass
from types import SimpleNamespace
from tempfile import NamedTemporaryFile

import jwt
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ed25519

from sqa.worker.api import Limit, QueryResource
from sqa.worker.auth import (
    API_KEY_ID_HEADER,
    AUTH_HEADER,
    MissingWorkerAuthToken,
    USER_ID_HEADER,
    VerifiedIdentity,
    WorkerAuthConfig,
    WorkerAuthError,
    WorkerAuthenticator,
)
from sqa.worker.query import QueryResult
from sqa.worker.server import get_worker_auth_config, parse_ip_networks
from sqa.worker.state.dataset import dataset_encode


@dataclass
class FakeRequest:
    headers: dict[str, str]
    requested_headers: list[str]
    content_length: int | None = 2
    content_type: str | None = 'application/json'
    params: dict[str, str] = None

    def __init__(
        self,
        headers: dict[str, str],
        media: dict | None = None,
        remote_addr: str | None = None,
        access_route: list[str] | None = None,
        peer_addr: str | None = None,
    ):
        self.headers = headers
        self.requested_headers = []
        self.params = {}
        self._media = media or {}
        self.remote_addr = remote_addr
        self.access_route = access_route
        self.scope = {}
        if peer_addr is not None:
            self.scope['client'] = (peer_addr, 12345)

    def get_header(self, name: str) -> str | None:
        self.requested_headers.append(name)
        return self.headers.get(name)

    async def get_media(self):
        return self._media


class FakeResponse:
    def __init__(self):
        self.headers = {}
        self.data = None
        self.content_type = None

    def set_header(self, name: str, value: str):
        self.headers[name] = value


class FakeWorker:
    async def execute_query(self, query, dataset, profiling=False):
        return QueryResult(
            compressed_data=b'{}',
            data_size=2,
            data_sha3_256=None,
            num_read_chunks=1,
        )


class WorkerAuthTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        key = ed25519.Ed25519PrivateKey.generate()
        self.private_key = key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption()
        ).decode()
        self.public_key = key.public_key().public_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PublicFormat.SubjectPublicKeyInfo
        ).decode()
        self.now = int(time.time())

    async def test_valid_jwt_is_accepted(self):
        identity = await self._verify(self._token())

        self.assertEqual(identity, VerifiedIdentity('user-1', 'key-1'))

    async def test_request_header_identity_is_decoded(self):
        token = self._token({'u': 'user-2', 'k': 'key-2'})
        auth = self._authenticator()

        identity = await auth.verify_request(FakeRequest({AUTH_HEADER: token}))

        self.assertEqual(identity, VerifiedIdentity('user-2', 'key-2'))

    async def test_missing_token_is_rejected(self):
        auth = self._authenticator()

        with self.assertRaises(MissingWorkerAuthToken):
            await auth.verify_request(FakeRequest({}))

    async def test_auth_disabled_skips_worker_auth_header(self):
        auth = WorkerAuthenticator(WorkerAuthConfig(enabled=False))
        req = FakeRequest({})

        identity = await auth.verify_request(req)

        self.assertIsNone(identity)
        self.assertEqual(req.requested_headers, [])

    async def test_auth_disabled_skips_worker_auth_header_even_when_public_key_is_provided(self):
        token = self._token({'u': 'user-2', 'k': 'key-2'})
        auth = WorkerAuthenticator(WorkerAuthConfig(
            enabled=False,
            public_key=self.public_key
        ))
        req = FakeRequest({AUTH_HEADER: token})

        identity = await auth.verify_request(req)

        self.assertIsNone(identity)
        self.assertEqual(req.requested_headers, [])

    async def test_auth_disabled_does_not_load_invalid_public_key(self):
        auth = WorkerAuthenticator(WorkerAuthConfig(
            enabled=False,
            public_key='not a public key'
        ))
        req = FakeRequest({AUTH_HEADER: self._token()})

        identity = await auth.verify_request(req)

        self.assertIsNone(identity)
        self.assertEqual(req.requested_headers, [])

    async def test_auth_enabled_does_not_reject_invalid_worker_auth_header_for_non_enforced_ip(self):
        auth = WorkerAuthenticator(WorkerAuthConfig(
            enabled=True,
            public_key=self.public_key,
            enforce_for_ips=parse_ip_networks('10.0.0.0/8'),
        ))

        identity = await auth.verify_request(FakeRequest(
            {AUTH_HEADER: 'bad-token'},
            peer_addr='8.8.8.8',
        ))

        self.assertIsNone(identity)

    async def test_auth_enabled_rejects_missing_worker_auth_header_for_enforced_ip(self):
        auth = WorkerAuthenticator(WorkerAuthConfig(
            enabled=True,
            public_key=self.public_key,
            enforce_for_ips=parse_ip_networks('10.0.0.0/8'),
        ))

        with self.assertRaises(MissingWorkerAuthToken):
            await auth.verify_request(FakeRequest({}, peer_addr='10.4.5.5'))

    async def test_auth_enabled_rejects_invalid_worker_auth_header_for_enforced_ip(self):
        auth = WorkerAuthenticator(WorkerAuthConfig(
            enabled=True,
            public_key=self.public_key,
            enforce_for_ips=parse_ip_networks('10.0.0.0/8'),
        ))

        with self.assertRaises(WorkerAuthError):
            await auth.verify_request(FakeRequest(
                {AUTH_HEADER: 'bad-token'},
                peer_addr='10.4.5.5',
            ))

    async def test_auth_enabled_does_not_reject_missing_worker_auth_header_for_non_enforced_ip(self):
        auth = WorkerAuthenticator(WorkerAuthConfig(
            enabled=True,
            public_key=self.public_key,
            enforce_for_ips=parse_ip_networks('10.0.0.0/8'),
        ))

        identity = await auth.verify_request(FakeRequest({}, peer_addr='8.8.8.8'))

        self.assertIsNone(identity)

    async def test_auth_enabled_wildcard_enforces_without_client_ip(self):
        auth = WorkerAuthenticator(WorkerAuthConfig(
            enabled=True,
            public_key=self.public_key,
            enforce_for_ips=parse_ip_networks('*'),
        ))

        with self.assertRaises(MissingWorkerAuthToken):
            await auth.verify_request(FakeRequest({}))

    async def test_auth_enabled_catch_all_cidr_enforces_without_client_ip(self):
        auth = WorkerAuthenticator(WorkerAuthConfig(
            enabled=True,
            public_key=self.public_key,
            enforce_for_ips=parse_ip_networks('0.0.0.0/0'),
        ))

        with self.assertRaises(MissingWorkerAuthToken):
            await auth.verify_request(FakeRequest({}))

    async def test_auth_enabled_enforce_scope_uses_original_forwarded_for_with_trusted_ips(self):
        auth = WorkerAuthenticator(WorkerAuthConfig(
            enabled=True,
            public_key=self.public_key,
            enforce_for_ips=parse_ip_networks('94.43.0.0/16'),
            trusted_ips=parse_ip_networks('34.149.211.238'),
        ))

        with self.assertRaises(MissingWorkerAuthToken):
            await auth.verify_request(FakeRequest(
                {'x-original-forwarded-for': '94.43.76.236, 34.149.211.238'},
                peer_addr='10.6.2.3',
            ))

    async def test_auth_enabled_xoff_without_trusted_ips_uses_rightmost_ip(self):
        auth = WorkerAuthenticator(WorkerAuthConfig(
            enabled=True,
            public_key=self.public_key,
            enforce_for_ips=parse_ip_networks('94.43.0.0/16'),
        ))

        identity = await auth.verify_request(FakeRequest(
            {'x-original-forwarded-for': '94.43.76.236, 34.149.211.238'},
            peer_addr='10.6.2.3',
        ))

        self.assertIsNone(identity)

    async def test_auth_enabled_xoff_all_trusted_falls_back_to_raw_peer(self):
        auth = WorkerAuthenticator(WorkerAuthConfig(
            enabled=True,
            public_key=self.public_key,
            enforce_for_ips=parse_ip_networks('10.0.0.0/8'),
            trusted_ips=parse_ip_networks('34.149.211.238'),
        ))

        with self.assertRaises(MissingWorkerAuthToken):
            await auth.verify_request(FakeRequest(
                {'x-original-forwarded-for': '34.149.211.238'},
                peer_addr='10.6.2.3',
            ))

    async def test_bare_zero_ip_is_not_catch_all(self):
        auth = WorkerAuthenticator(WorkerAuthConfig(
            enabled=True,
            public_key=self.public_key,
            enforce_for_ips=parse_ip_networks('0.0.0.0'),
        ))

        identity = await auth.verify_request(FakeRequest({}, peer_addr='8.8.8.8'))

        self.assertIsNone(identity)

    async def test_auth_enabled_ignores_spoofable_forwarded_headers_for_peer_fallback(self):
        auth = WorkerAuthenticator(WorkerAuthConfig(
            enabled=True,
            public_key=self.public_key,
            enforce_for_ips=parse_ip_networks('10.0.0.0/8'),
        ))

        identity = await auth.verify_request(FakeRequest(
            {
                'x-forwarded-for': '10.4.5.5',
                'x-real-ip': '10.4.5.5',
            },
            access_route=['10.4.5.5'],
            peer_addr='8.8.8.8',
        ))

        self.assertIsNone(identity)

    async def test_auth_enabled_narrow_scope_does_not_enforce_without_raw_peer(self):
        auth = WorkerAuthenticator(WorkerAuthConfig(
            enabled=True,
            public_key=self.public_key,
            enforce_for_ips=parse_ip_networks('10.0.0.0/8'),
        ))

        identity = await auth.verify_request(FakeRequest(
            {},
            remote_addr='10.4.5.5',
            access_route=['10.4.5.5'],
        ))

        self.assertIsNone(identity)

    async def test_query_response_includes_decoded_auth_headers_when_auth_is_enabled(self):
        token = self._token({'u': 'user-2', 'k': 'key-2'})
        auth = WorkerAuthenticator(WorkerAuthConfig(
            enabled=True,
            public_key=self.public_key
        ))
        resource = QueryResource(FakeWorker(), Limit(1), auth)
        req = FakeRequest({AUTH_HEADER: token}, media={'fromBlock': 1})
        res = FakeResponse()

        await resource.on_post(req, res, dataset_encode('test-dataset'))

        self.assertEqual(res.headers[USER_ID_HEADER], 'user-2')
        self.assertEqual(res.headers[API_KEY_ID_HEADER], 'key-2')

    async def test_query_response_skips_unsafe_decoded_auth_headers(self):
        token = self._token({'u': 'user\n2', 'k': 'key-2'})
        auth = WorkerAuthenticator(WorkerAuthConfig(
            enabled=True,
            public_key=self.public_key
        ))
        resource = QueryResource(FakeWorker(), Limit(1), auth)
        req = FakeRequest({AUTH_HEADER: token}, media={'fromBlock': 1})
        res = FakeResponse()

        await resource.on_post(req, res, dataset_encode('test-dataset'))

        self.assertNotIn(USER_ID_HEADER, res.headers)
        self.assertEqual(res.headers[API_KEY_ID_HEADER], 'key-2')

    async def test_query_response_skips_non_latin1_decoded_auth_headers(self):
        token = self._token({'u': 'user-2', 'k': 'ключ-2'})
        auth = WorkerAuthenticator(WorkerAuthConfig(
            enabled=True,
            public_key=self.public_key
        ))
        resource = QueryResource(FakeWorker(), Limit(1), auth)
        req = FakeRequest({AUTH_HEADER: token}, media={'fromBlock': 1})
        res = FakeResponse()

        await resource.on_post(req, res, dataset_encode('test-dataset'))

        self.assertEqual(res.headers[USER_ID_HEADER], 'user-2')
        self.assertNotIn(API_KEY_ID_HEADER, res.headers)

    async def test_auth_enabled_requires_public_key(self):
        with self.assertRaisesRegex(ValueError, 'public key'):
            WorkerAuthenticator(WorkerAuthConfig(enabled=True))

    async def test_invalid_public_key_is_rejected_at_startup(self):
        with self.assertRaisesRegex(ValueError, 'public key'):
            WorkerAuthenticator(WorkerAuthConfig(
                enabled=True,
                public_key='not a public key'
            ))

    async def test_public_key_can_be_passed_as_pem(self):
        auth = WorkerAuthenticator(WorkerAuthConfig(
            enabled=True,
            public_key=self.public_key
        ))

        identity = await auth.verify_token(self._token())

        self.assertEqual(identity, VerifiedIdentity('user-1', 'key-1'))

    async def test_public_key_can_be_passed_as_path(self):
        with NamedTemporaryFile(mode='w') as f:
            f.write(self.public_key)
            f.flush()
            auth = WorkerAuthenticator(WorkerAuthConfig(
                enabled=True,
                public_key=f.name
            ))

        identity = await auth.verify_token(self._token())

        self.assertEqual(identity, VerifiedIdentity('user-1', 'key-1'))

    async def test_expired_token_is_rejected(self):
        token = self._token({'exp': self.now - 1})

        with self.assertRaisesRegex(WorkerAuthError, 'invalid|expired'):
            await self._verify(token)

    async def test_not_yet_valid_token_is_rejected(self):
        token = self._token({'nbf': self.now + 60})

        with self.assertRaisesRegex(WorkerAuthError, 'invalid'):
            await self._verify(token)

    async def test_invalid_signature_is_rejected(self):
        other = ed25519.Ed25519PrivateKey.generate()
        other_private_key = other.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption()
        ).decode()
        token = self._token(private_key=other_private_key)

        with self.assertRaisesRegex(WorkerAuthError, 'invalid'):
            await self._verify(token)

    async def test_wrong_algorithm_is_rejected(self):
        token = jwt.encode(
            self._claims(),
            key='secret',
            algorithm='HS256',
            headers={'typ': None}
        )

        with self.assertRaisesRegex(WorkerAuthError, 'algorithm'):
            await self._verify(token)

    async def test_minimal_header_is_accepted(self):
        token = self._token(headers={'typ': None})
        header = jwt.get_unverified_header(token)

        self.assertEqual(header, {'alg': 'EdDSA'})

        identity = await self._verify(token)

        self.assertEqual(identity.user_id, 'user-1')

    async def test_required_claims_are_enforced(self):
        for claim in ['u', 'k', 'iat', 'exp']:
            claims = self._claims()
            del claims[claim]
            token = jwt.encode(
                claims,
                key=self.private_key,
                algorithm='EdDSA',
                headers={'typ': None}
            )

            with self.subTest(claim=claim):
                with self.assertRaises(WorkerAuthError):
                    await self._verify(token)

    async def test_future_iat_beyond_clock_skew_is_rejected(self):
        token = self._token({'iat': self.now + 120})

        with self.assertRaisesRegex(WorkerAuthError, 'future'):
            await self._verify(token)

    async def test_enforce_for_ips_requires_public_key_when_auth_is_not_disabled(self):
        with self.assertRaisesRegex(ValueError, 'public key'):
            get_worker_auth_config(SimpleNamespace(
                auth_enabled=None,
                disable_v2_auth=False,
                auth_public_key=None,
                enforce_v2_auth_for_ips=parse_ip_networks('*'),
                trusted_ips=(),
            ))

    async def test_disabled_auth_allows_enforce_for_ips_without_public_key(self):
        config = get_worker_auth_config(SimpleNamespace(
            auth_enabled=None,
            disable_v2_auth=True,
            auth_public_key=None,
            enforce_v2_auth_for_ips=parse_ip_networks('*'),
            trusted_ips=(),
        ))

        self.assertFalse(config.enabled)

    async def _verify(
        self,
        token: str
    ) -> VerifiedIdentity:
        return await self._authenticator().verify_token(token)

    def _authenticator(self) -> WorkerAuthenticator:
        return WorkerAuthenticator(WorkerAuthConfig(
            enabled=True,
            public_key=self.public_key,
            enforce_for_ips=parse_ip_networks('*'),
        ))

    def _token(
        self,
        claims: dict | None = None,
        headers: dict | None = None,
        private_key: str | None = None
    ) -> str:
        if claims is None:
            claims = self._claims()
        elif set(claims).issubset({'u', 'k', 'iat', 'exp', 'nbf'}):
            claims = {**self._claims(), **claims}

        if headers is None:
            headers = {'typ': None}

        return jwt.encode(
            claims,
            key=private_key or self.private_key,
            algorithm='EdDSA',
            headers=headers
        )

    def _claims(self) -> dict:
        return {
            'u': 'user-1',
            'k': 'key-1',
            'iat': self.now,
            'exp': self.now + 60,
        }


if __name__ == '__main__':
    unittest.main()
