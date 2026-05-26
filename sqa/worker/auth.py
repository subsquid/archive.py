import os
import time
from dataclasses import dataclass
from typing import Any

import jwt
from cryptography.hazmat.primitives import serialization


AUTH_HEADER = 'x-sqd-auth'
DISABLED_IDENTITY_VALUE = 'disabled'


@dataclass(frozen=True)
class VerifiedIdentity:
    user_id: str
    api_key_id: str


@dataclass(frozen=True)
class WorkerAuthConfig:
    enabled: bool = False
    public_key: str | None = None

    def validate(self) -> None:
        if not self.enabled:
            return

        if self.public_key is None:
            raise ValueError('worker auth requires a public key')


class WorkerAuthError(Exception):
    pass


class MissingWorkerAuthToken(WorkerAuthError):
    pass


class WorkerAuthenticator:
    def __init__(self, config: WorkerAuthConfig):
        config.validate()
        self._config = config
        self._public_key: str | None = None
        if self._config.enabled:
            self._load_public_key()

    async def verify_request(self, req: Any) -> VerifiedIdentity:
        if not self._config.enabled:
            return VerifiedIdentity(
                user_id=DISABLED_IDENTITY_VALUE,
                api_key_id=DISABLED_IDENTITY_VALUE
            )

        token = req.get_header(AUTH_HEADER)
        if not token:
            raise MissingWorkerAuthToken('obtain worker auth token from router')

        return await self.verify_token(token)

    async def verify_token(self, token: str) -> VerifiedIdentity:
        try:
            header = jwt.get_unverified_header(token)
        except jwt.PyJWTError as e:
            raise WorkerAuthError('malformed worker auth token') from e

        if header.get('alg') != 'EdDSA':
            raise WorkerAuthError('invalid worker auth token algorithm')

        try:
            claims = jwt.decode(
                token,
                key=self._load_public_key(),
                algorithms=['EdDSA'],
                options={
                    'require': ['u', 'k', 'iat', 'exp'],
                    'verify_iat': False,
                }
            )
        except jwt.PyJWTError as e:
            raise WorkerAuthError('invalid worker auth token') from e

        user_id = claims.get('u')
        api_key_id = claims.get('k')
        iat = claims.get('iat')
        exp = claims.get('exp')

        if not isinstance(user_id, str) or not user_id:
            raise WorkerAuthError('worker auth token is missing u')

        if not isinstance(api_key_id, str) or not api_key_id:
            raise WorkerAuthError('worker auth token is missing k')

        now = time.time()
        if not _is_number(exp) or exp <= now:
            raise WorkerAuthError('worker auth token is expired')

        if not _is_number(iat):
            raise WorkerAuthError('worker auth token is missing iat')

        if iat > now + 30:
            raise WorkerAuthError('worker auth token was issued in the future')

        return VerifiedIdentity(user_id=user_id, api_key_id=api_key_id)

    def _load_public_key(self) -> str:
        if self._public_key is None:
            assert self._config.public_key is not None
            try:
                self._public_key = _read_public_key(self._config.public_key)
                serialization.load_pem_public_key(self._public_key.encode())
            except (OSError, ValueError) as e:
                raise ValueError('invalid worker auth public key') from e
        return self._public_key


def _read_public_key(value: str) -> str:
    if value.startswith('file:'):
        with open(value[5:]) as f:
            return f.read()

    if os.path.exists(value):
        with open(value) as f:
            return f.read()

    return value


def _is_number(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)
