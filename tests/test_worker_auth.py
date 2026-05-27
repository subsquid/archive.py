import time
import unittest
from dataclasses import dataclass
from tempfile import NamedTemporaryFile

import jwt
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ed25519

from sqa.worker.auth import (
    AUTH_HEADER,
    MissingWorkerAuthToken,
    VerifiedIdentity,
    WorkerAuthConfig,
    WorkerAuthError,
    WorkerAuthenticator,
)


@dataclass
class FakeRequest:
    headers: dict[str, str]
    requested_headers: list[str]

    def __init__(self, headers: dict[str, str]):
        self.headers = headers
        self.requested_headers = []

    def get_header(self, name: str) -> str | None:
        self.requested_headers.append(name)
        return self.headers.get(name)


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

    async def _verify(
        self,
        token: str
    ) -> VerifiedIdentity:
        return await self._authenticator().verify_token(token)

    def _authenticator(self) -> WorkerAuthenticator:
        return WorkerAuthenticator(WorkerAuthConfig(
            enabled=True,
            public_key=self.public_key
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
