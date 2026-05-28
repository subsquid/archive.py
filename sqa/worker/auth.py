import os
import time
from dataclasses import dataclass
from ipaddress import IPv4Network, IPv6Network, ip_address
from typing import Any

import jwt
from cryptography.hazmat.primitives import serialization


AUTH_HEADER = 'x-sqd-auth'
USER_ID_HEADER = 'x-sqd-user-id'
API_KEY_ID_HEADER = 'x-sqd-api-key-id'
ORIGINAL_FORWARDED_FOR_HEADER = 'x-original-forwarded-for'
IpNetwork = IPv4Network | IPv6Network


@dataclass(frozen=True)
class VerifiedIdentity:
    user_id: str
    api_key_id: str


@dataclass(frozen=True)
class WorkerAuthConfig:
    enabled: bool = False
    public_key: str | None = None
    enforce_for_ips: tuple[IpNetwork, ...] = ()
    trusted_ips: tuple[IpNetwork, ...] = ()

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

    async def verify_request(self, req: Any) -> VerifiedIdentity | None:
        if not self._config.enabled:
            return None

        enforce = self._should_enforce(req)
        token = req.get_header(AUTH_HEADER)
        if not token:
            if enforce:
                raise MissingWorkerAuthToken('obtain worker auth token from router')
            return None

        try:
            return await self.verify_token(token)
        except WorkerAuthError:
            if enforce:
                raise
            return None

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

    def _should_enforce(self, req: Any) -> bool:
        if not self._config.enforce_for_ips:
            return False

        client_ip = _get_client_ip(req, self._config.trusted_ips)
        if client_ip is None:
            return _has_catch_all(self._config.enforce_for_ips)

        return _contains_ip(self._config.enforce_for_ips, client_ip)

    def _load_public_key(self) -> str:
        if self._public_key is None:
            assert self._config.public_key is not None
            try:
                self._public_key = _read_public_key(self._config.public_key)
                serialization.load_pem_public_key(self._public_key.encode())
            except (OSError, ValueError) as e:
                raise ValueError('invalid worker auth public key') from e
        return self._public_key


def _get_client_ip(req: Any, trusted_ips: tuple[IpNetwork, ...]):
    chain = req.get_header(ORIGINAL_FORWARDED_FOR_HEADER)
    if chain:
        for segment in reversed(chain.split(',')):
            try:
                ip = ip_address(segment.strip())
            except ValueError:
                continue
            if not _contains_ip(trusted_ips, ip):
                return ip

    value = _get_peer_ip(req)
    if value:
        try:
            return ip_address(value)
        except ValueError:
            pass

    return None


def _contains_ip(networks: tuple[IpNetwork, ...], ip) -> bool:
    return any(net.version == ip.version and ip in net for net in networks)


def _get_peer_ip(req: Any) -> str | None:
    client = getattr(req, 'scope', {}).get('client')
    if client:
        return client[0]
    return None


def _has_catch_all(networks: tuple[IpNetwork, ...]) -> bool:
    return any(net.prefixlen == 0 for net in networks)


def _read_public_key(value: str) -> str:
    if os.path.exists(value):
        with open(value) as f:
            return f.read()

    return value


def _is_number(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)
