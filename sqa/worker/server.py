import argparse
import asyncio
import logging
import os
from ipaddress import ip_network

import falcon.asgi as fa
import httpx
import uvicorn

from sqa.util.asyncio import create_child_task, monitor_service_tasks, run_async_program
from sqa.worker.api import StatusResource, QueryResource, Limit
from sqa.worker.auth import IpNetwork, WorkerAuthConfig, WorkerAuthenticator
from sqa.worker.state.controller import State
from sqa.worker.state.intervals import to_range_set
from sqa.worker.state.manager import StateManager
from sqa.worker.worker import Worker


LOG = logging.getLogger(__name__)


def parse_ip_networks(value: str | None) -> tuple[IpNetwork, ...]:
    if not value:
        return ()

    networks = []
    for item in value.split(','):
        item = item.strip()
        if not item:
            continue
        if item == '*':
            networks.append(ip_network('0.0.0.0/0'))
            networks.append(ip_network('::/0'))
            continue
        networks.append(ip_network(item, strict=False))
    return tuple(networks)


class HttpTransport:
    def __init__(self,  worker_id: str, worker_url: str, router_url: str):
        self._worker_id = worker_id
        self._worker_url = worker_url
        self._router_url = router_url
        self._state_updates = asyncio.Queue(maxsize=100)

    async def send_ping(self, state: State, stored_bytes: int, pause=False):
        ping_msg = {
            'worker_id': self._worker_id,
            'worker_url': self._worker_url + '/query',
            'state': state,
            'pause': pause,
        }

        async with httpx.AsyncClient(base_url=self._router_url) as client:
            try:
                response = await client.post('/ping', json=ping_msg)
                response.raise_for_status()
                result = response.json()
                desired_state = {
                    ds: to_range_set(map(tuple, ranges)) for ds, ranges in result.items()
                }
                await self._state_updates.put(desired_state)
            except httpx.HTTPError:
                LOG.exception('failed to send a ping message')

    async def state_updates(self):
        while True:
            yield await self._state_updates.get()


class Server(uvicorn.Server):
    def install_signal_handlers(self) -> None:
        pass

    async def run_server_task(self) -> None:
        task = create_child_task('serve', self.serve())
        try:
            # wrap with `asyncio.wait()` to prevent immediate coroutine abortion
            await asyncio.wait([task])
        except asyncio.CancelledError:
            self.should_exit = True
            await task


def parse_cli_args():
    program = argparse.ArgumentParser(
        description='Subsquid eth archive worker'
    )

    program.add_argument(
        '--router',
        required=True,
        metavar='URL',
        help='URL of the router to connect to'
    )

    program.add_argument(
        '--worker-id',
        required=True,
        metavar='UID',
        help='unique id of this worker'
    )

    program.add_argument(
        '--worker-url',
        required=True,
        metavar='URL',
        help='externally visible URL of this worker'
    )

    program.add_argument(
        '--data-dir',
        metavar='DIR',
        help='directory to keep in the data and state of this worker (defaults to cwd)'
    )

    program.add_argument(
        '--port',
        type=int,
        default=8000,
        metavar='N',
        help='port to listen on (defaults to 8000)'
    )

    program.add_argument(
        '--procs',
        type=int,
        metavar='N',
        help='number of processes to use to execute data queries'
    )

    program.add_argument(
        '--auth-enabled',
        action=argparse.BooleanOptionalAction,
        default=None,
        help=argparse.SUPPRESS
    )

    program.add_argument(
        '--disable-v2-auth',
        action=argparse.BooleanOptionalAction,
        default=False,
        help='skip worker auth validation entirely'
    )

    program.add_argument(
        '--auth-public-key',
        metavar='KEY',
        help='worker JWT verification public key PEM or filesystem path'
    )

    program.add_argument(
        '--enforce-v2-auth-for-ips',
        dest='enforce_v2_auth_for_ips',
        type=parse_ip_networks,
        default=(),
        metavar='CIDR,CIDR,...',
        help='client IP CIDRs where worker auth failures reject requests; use * for all'
    )

    program.add_argument(
        '--auth-enforce-for-ips',
        dest='enforce_v2_auth_for_ips',
        type=parse_ip_networks,
        help=argparse.SUPPRESS
    )

    program.add_argument(
        '--trusted-ips',
        dest='trusted_ips',
        type=parse_ip_networks,
        default=(),
        metavar='CIDR,CIDR,...',
        help='trusted proxy CIDRs stripped from the right side of X-Original-Forwarded-For'
    )

    program.add_argument(
        '--auth-trusted-ips',
        dest='trusted_ips',
        type=parse_ip_networks,
        help=argparse.SUPPRESS
    )

    return program.parse_args()


def get_worker_auth_config(args) -> WorkerAuthConfig:
    auth_enabled = args.auth_enabled
    if auth_enabled is None:
        auth_enabled = args.auth_public_key is not None

    if (
        args.enforce_v2_auth_for_ips
        and not args.disable_v2_auth
        and args.auth_public_key is None
    ):
        raise ValueError('worker auth public key is required when enforcing v2 auth')

    return WorkerAuthConfig(
        enabled=auth_enabled and not args.disable_v2_auth,
        public_key=args.auth_public_key,
        enforce_for_ips=args.enforce_v2_auth_for_ips,
        trusted_ips=args.trusted_ips
    )


async def serve(args):
    data_dir = args.data_dir or os.getcwd()
    sm = StateManager(data_dir=data_dir)

    transport = HttpTransport(
        worker_id=args.worker_id,
        worker_url=args.worker_url,
        router_url=args.router,
    )

    worker = Worker(sm, transport, args.procs)
    limit = Limit(worker.get_processes_count() * 3)
    authenticator = WorkerAuthenticator(get_worker_auth_config(args))

    app = fa.App()

    app.add_route('/status', StatusResource(
        sm=sm,
        router_url=args.router,
        worker_id=args.worker_id,
        worker_url=args.worker_url,
        limit=limit
    ))

    app.add_route('/query/{dataset}', QueryResource(worker, limit, authenticator))

    server_conf = uvicorn.Config(
        app,
        port=args.port,
        host='0.0.0.0',
        proxy_headers=False,
        access_log=False,
        log_config=None
    )

    server = Server(server_conf)

    await monitor_service_tasks([
        asyncio.create_task(server.run_server_task(), name='http_server'),
        asyncio.create_task(sm.run(), name='state_manager'),
        asyncio.create_task(worker.run(), name='worker'),
    ])


def cli():
    if os.getenv('SENTRY_DSN'):
        import sentry_sdk
        sentry_sdk.init(
            traces_sample_rate=1.0
        )
    run_async_program(serve, parse_cli_args())
