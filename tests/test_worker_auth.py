import time
import unittest
from dataclasses import dataclass

import jwt
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa

from sqa.worker.auth import (
    DISABLED_IDENTITY_VALUE,
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

    def get_header(self, name: str) -> str | None:
        return self.headers.get(name)


class WorkerAuthTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
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
        token = self._token({'user_id': 'user-2', 'api_key_id': 'key-2'})
        auth = self._authenticator()

        identity = await auth.verify_request(FakeRequest({AUTH_HEADER: token}))

        self.assertEqual(identity, VerifiedIdentity('user-2', 'key-2'))

    async def test_missing_token_is_rejected(self):
        auth = self._authenticator()

        with self.assertRaises(MissingWorkerAuthToken):
            await auth.verify_request(FakeRequest({}))

    async def test_auth_disabled_uses_placeholder_identity(self):
        auth = WorkerAuthenticator(WorkerAuthConfig(enabled=False))

        identity = await auth.verify_request(FakeRequest({}))

        self.assertEqual(identity.user_id, DISABLED_IDENTITY_VALUE)
        self.assertEqual(identity.api_key_id, DISABLED_IDENTITY_VALUE)

    async def test_auth_enabled_requires_public_key(self):
        with self.assertRaisesRegex(ValueError, 'public key'):
            WorkerAuthenticator(WorkerAuthConfig(enabled=True))

    async def test_invalid_public_key_is_rejected_at_startup(self):
        with self.assertRaisesRegex(ValueError, 'public key'):
            WorkerAuthenticator(WorkerAuthConfig(
                enabled=True,
                public_key='not a public key'
            ))

    async def test_expired_token_is_rejected(self):
        token = self._token({'exp': self.now - 1})

        with self.assertRaisesRegex(WorkerAuthError, 'invalid|expired'):
            await self._verify(token)

    async def test_not_yet_valid_token_is_rejected(self):
        token = self._token({'nbf': self.now + 60})

        with self.assertRaisesRegex(WorkerAuthError, 'invalid'):
            await self._verify(token)

    async def test_invalid_signature_is_rejected(self):
        other = rsa.generate_private_key(public_exponent=65537, key_size=2048)
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
            headers={'kid': 'kid-1'}
        )

        with self.assertRaisesRegex(WorkerAuthError, 'algorithm'):
            await self._verify(token)

    async def test_token_kid_is_ignored(self):
        identity = await self._verify(self._token(headers={'kid': 'other'}))

        self.assertEqual(identity.user_id, 'user-1')

    async def test_missing_kid_is_accepted(self):
        identity = await self._verify(self._token(headers={}))

        self.assertEqual(identity.user_id, 'user-1')

    async def test_required_claims_are_enforced(self):
        for claim in ['user_id', 'api_key_id', 'iat', 'exp']:
            claims = self._claims()
            del claims[claim]
            token = jwt.encode(
                claims,
                key=self.private_key,
                algorithm='RS256',
                headers={'kid': 'kid-1'}
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
        elif set(claims).issubset({'user_id', 'api_key_id', 'iat', 'exp', 'nbf'}):
            claims = {**self._claims(), **claims}

        if headers is None:
            headers = {'kid': 'kid-1'}

        return jwt.encode(
            claims,
            key=private_key or self.private_key,
            algorithm='RS256',
            headers=headers
        )

    def _claims(self) -> dict:
        return {
            'user_id': 'user-1',
            'api_key_id': 'key-1',
            'iat': self.now,
            'exp': self.now + 60,
        }


if __name__ == '__main__':
    unittest.main()
