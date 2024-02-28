import argparse
import asyncio
import json
import logging
import os
from json import JSONDecodeError
from typing import AsyncIterator, Optional, Dict, Iterator

import grpc.aio
from google.protobuf import empty_pb2
from marshmallow import ValidationError

from sqa.gateway_allocations.gateway_allocations import GatewayAllocations
from sqa.metrics import WORKER_INFO, WORKER_STATUS, STORED_BYTES, EXEC_TIME, RESULT_SIZE, READ_CHUNKS, QUERY_OK, \
    BAD_REQUEST, SERVER_ERROR, MetricsServer
from sqa.query import MissingData
from sqa.util.asyncio import create_child_task, monitor_service_tasks, run_async_program
from sqa.worker.p2p import messages_pb2 as msg_pb
from sqa.worker.p2p.query_logs import LogsStorage
from sqa.worker.p2p.rpc import RPCWrapper
from sqa.worker.p2p.util import state_to_proto, state_from_proto, QueryInfo
from sqa.worker.query import QueryResult, InvalidQuery
from sqa.worker.state.controller import State
from sqa.worker.state.dataset import dataset_decode
from sqa.worker.state.manager import StateManager
from sqa.worker.util import sha3_256
from sqa.worker.worker import Worker


LOG = logging.getLogger(__name__)

# This is the maximum *decompressed* size of the message
MAX_MESSAGE_LENGTH = 100 * 1024 * 1024  # 100 MiB
LOGS_MESSAGE_MAX_BYTES = 64000
LOGS_SEND_INTERVAL_SEC = int(os.environ.get('LOGS_SEND_INTERVAL_SEC', '600'))
PING_TOPIC = "worker_ping"
LOGS_TOPIC = "worker_query_logs"
WORKER_VERSION = "0.2.3"


def bundle_logs(logs: list[msg_pb.QueryExecuted]) -> Iterator[msg_pb.Envelope]:
    while logs:
        bundle = []
        bundle_size = 0
        while logs and (log := logs.pop(0)).ByteSize() + bundle_size < LOGS_MESSAGE_MAX_BYTES:
            bundle.append(log)
            bundle_size += log.ByteSize()
        if bundle:
            yield msg_pb.Envelope(query_logs=msg_pb.QueryLogs(queries_executed=bundle))
        else:
            LOG.error("Query log too big to be sent")
            logs.pop(0)


class P2PTransport:
    def __init__(self, channel: grpc.aio.Channel, scheduler_id: str, logs_collector_id: str):
        self._rpc = RPCWrapper(channel)
        self._scheduler_id = scheduler_id
        self._logs_collector_id = logs_collector_id
        self._state_updates = asyncio.Queue(maxsize=100)
        self._query_tasks = asyncio.Queue(maxsize=100)
        self._query_info: 'Dict[str, QueryInfo]' = {}
        self._logs_storage: 'Optional[LogsStorage]' = None
        self._local_peer_id: 'Optional[str]' = None
        self._expected_pong: 'Optional[bytes]' = None  # Hash of the last sent ping

    async def initialize(self, logs_db_path: str) -> str:
        self._local_peer_id = await self._rpc.local_peer_id()
        LOG.info(f"Local peer ID: {self._local_peer_id}")
        WORKER_INFO.info({
            'peer_id': self._local_peer_id,
            'version': WORKER_VERSION,
        })
        WORKER_STATUS.state('starting')
        self._logs_storage = LogsStorage(self._local_peer_id, logs_db_path)

        # Subscribe to the ping & logs topics so that our node also participates in broadcasting messages
        await self._rpc.subscribe(PING_TOPIC)
        await self._rpc.subscribe(LOGS_TOPIC)

        return self._local_peer_id

    async def run(self, gateway_allocations: GatewayAllocations):
        receive_task = create_child_task('p2p_receive', self._receive_loop(gateway_allocations))
        send_logs_task = create_child_task('p2p_send_logs', self._send_logs_loop())
        await monitor_service_tasks([receive_task, send_logs_task], log=LOG)

    async def _receive_loop(self, gateway_allocations: GatewayAllocations) -> None:
        async for peer_id, envelope in self._rpc.get_messages():
            try:
                msg_type = envelope.WhichOneof('msg')

                if msg_type == 'pong':
                    await self._handle_pong(peer_id, envelope.pong)

                elif msg_type == 'query':
                    await self._handle_query(peer_id, envelope.query, gateway_allocations)

                elif msg_type == 'logs_collected':
                    if peer_id != self._logs_collector_id:
                        LOG.warning(f"Wrong next_seq_no message origin: {peer_id}")
                        continue
                    last_collected_seq_no = envelope.logs_collected.sequence_numbers.get(self._local_peer_id)
                    self._logs_storage.logs_collected(last_collected_seq_no)

                else:
                    continue  # Just ignore pings and logs from other workers
            except Exception as e:
                LOG.exception(f"Message processing failed: {envelope}")
                SERVER_ERROR.inc()

    async def _send_logs_loop(self) -> None:
        while True:
            await asyncio.sleep(LOGS_SEND_INTERVAL_SEC)
            if not self._logs_storage.is_initialized:
                continue

            # Sign all the logs
            query_logs = self._logs_storage.get_logs()
            for log in query_logs:
                await self._rpc.sign_msg(log)

            # Send out
            LOG.debug("Sending out query logs")
            for envelope in bundle_logs(query_logs):
                await self._rpc.send_msg(envelope, topic=LOGS_TOPIC)

    async def _handle_pong(self, peer_id: str, msg: msg_pb.Pong) -> None:
        if peer_id != self._scheduler_id:
            LOG.warning(f"Wrong pong message origin: {peer_id}")
            return
        if msg.ping_hash != self._expected_pong:
            LOG.warning("Invalid ping hash in received pong message")
            return
        self._expected_pong = None

        status = msg.WhichOneof('status')
        WORKER_STATUS.state(status)
        if status == 'not_registered':
            LOG.error("Worker not registered on chain")
        elif status == 'unsupported_version':
            LOG.error("Worker version not supported by the scheduler")
        elif status == 'jailed':
            LOG.warning(f"Worker jailed. Reason: {msg.jailed}")
        elif status == 'active':
            # If the status is 'active', it contains assigned chunks
            await self._state_updates.put(msg.active)

    async def _handle_query(
            self,
            client_peer_id: str,
            query: msg_pb.Query,
            gateway_allocations: GatewayAllocations
    ) -> None:
        if not self._logs_storage.is_initialized:
            LOG.warning("Logs storage not initialized. Cannot execute queries yet.")
            return

        # Verify query signature
        signature = query.signature
        query.signature = b""
        signed_data = query.SerializeToString()
        if not await self._rpc.verify_signature(signed_data, signature, client_peer_id):
            LOG.warning(f"Query with invalid signature received from {client_peer_id}")
            return
        query.signature = signature

        # Check if client has sufficient compute units allocated
        if not await gateway_allocations.try_to_execute(client_peer_id):
            LOG.warning(f"Not enough allocated for {client_peer_id}")
            envelope = msg_pb.Envelope(
                query_result=msg_pb.QueryResult(
                    query_id=query.query_id,
                    no_allocation=empty_pb2.Empty(),
                )
            )
            await self._rpc.send_msg(envelope, peer_id=client_peer_id)
            return

        self._query_info[query.query_id] = QueryInfo(client_peer_id)
        await self._query_tasks.put(query)

    async def send_ping(self, state: State, stored_bytes: int, pause=False) -> None:
        STORED_BYTES.set(stored_bytes)
        ping = msg_pb.Ping(
            worker_id=self._local_peer_id,
            stored_ranges=state_to_proto(state),
            stored_bytes=stored_bytes,
            version=WORKER_VERSION,
        )
        await self._rpc.sign_msg(ping)
        envelope = msg_pb.Envelope(ping=ping)

        # We expect pong to be delivered before the next ping is sent
        if self._expected_pong is not None:
            LOG.error("Pong message not received in time. The scheduler is not responding. Contact tech support.")
        self._expected_pong = sha3_256(ping.SerializeToString())

        await self._rpc.send_msg(envelope, topic=PING_TOPIC)

    async def state_updates(self) -> AsyncIterator[State]:
        while True:
            state = await self._state_updates.get()
            yield state_from_proto(state)

    async def query_tasks(self) -> AsyncIterator[msg_pb.Query]:
        while True:
            yield await self._query_tasks.get()

    async def send_query_result(self, query: msg_pb.Query, result: QueryResult) -> None:
        try:
            query_info = self._query_info.pop(query.query_id)
        except KeyError:
            LOG.error(f"Unknown query: {query.query_id}")
            return
        query_info.finished()

        EXEC_TIME.observe(query_info.exec_time_ms)
        RESULT_SIZE.observe(len(result.compressed_data))
        READ_CHUNKS.observe(result.num_read_chunks)

        envelope = msg_pb.Envelope(
            query_result=msg_pb.QueryResult(
                query_id=query.query_id,
                ok=msg_pb.OkResult(
                    data=result.compressed_data,
                    exec_plan=None,
                )
            )
        )
        await self._rpc.send_msg(envelope, peer_id=query_info.client_id)
        self._logs_storage.query_executed(query, query_info, result)

    async def send_query_error(
            self,
            query: msg_pb.Query,
            bad_request: Optional[str] = None,
            server_error: Optional[str] = None
    ) -> None:
        try:
            query_info = self._query_info.pop(query.query_id)
        except KeyError:
            LOG.error(f"Unknown query: {query.query_id}")
            return
        query_info.finished()

        envelope = msg_pb.Envelope(
            query_result=msg_pb.QueryResult(
                query_id=query.query_id,
                bad_request=bad_request,
                server_error=server_error,
            )
        )
        await self._rpc.send_msg(envelope, peer_id=query_info.client_id)
        self._logs_storage.query_error(query, query_info, bad_request, server_error)


async def execute_query(transport: P2PTransport, worker: Worker, query_task: msg_pb.Query):
    try:
        query = json.loads(query_task.query)
        dataset = dataset_decode(query_task.dataset)
        result = await worker.execute_query(
            query,
            dataset,
            compute_data_hash=True,
            profiling=query_task.profiling
        )
        LOG.info(f"Query {query_task.query_id} success")
        QUERY_OK.inc()
        await transport.send_query_result(query_task, result)
    except (JSONDecodeError, ValidationError, InvalidQuery, MissingData) as e:
        LOG.warning(f"Query {query_task.query_id} execution failed")
        BAD_REQUEST.inc()
        await transport.send_query_error(query_task, bad_request=str(e))
    except Exception as e:
        LOG.exception(f"Query {query_task.query_id} execution failed")
        SERVER_ERROR.inc()
        await transport.send_query_error(query_task, server_error=str(e))


async def run_queries(transport: P2PTransport, worker: Worker):
    async for query_task in transport.query_tasks():
        # FIXME: backpressure
        asyncio.create_task(execute_query(transport, worker, query_task))


async def _main():
    program = argparse.ArgumentParser(
        description='Subsquid eth archive worker'
    )
    program.add_argument(
        '--proxy',
        metavar='URL',
        default='localhost:50051',
        help='URL of the P2P proxy (subsquid network rpc node) to connect to'
    )
    program.add_argument(
        '--scheduler-id',
        required=True,
        help='Peer ID of the scheduler'
    )
    program.add_argument(
        '--logs-collector-id',
        required=True,
        help='Peer ID of the logs collector'
    )
    program.add_argument(
        '--data-dir',
        metavar='DIR',
        help='directory to keep in the data and state of this worker (defaults to cwd)'
    )
    program.add_argument(
        '--procs',
        type=int,
        help='number of processes to use to execute data queries'
    )
    program.add_argument(
        '--prometheus-port',
        type=int,
        default=9090,
        help='port to expose Prometheus metrics'
    )
    program.add_argument(
        '--rpc-url',
        type=str,
        metavar='URL',
        required=True,
        help='URL address of an EVM RPC node'
    )
    args = program.parse_args()
    data_dir = args.data_dir or os.getcwd()
    chunks_dir = os.path.join(data_dir, 'worker')
    logs_db_path = os.path.join(data_dir, 'logs.db')
    allocations_db_path = os.path.join(data_dir, 'allocations.db')

    LOG.info(f"Exposing Prometheus metrics on port {args.prometheus_port}")
    MetricsServer.start(args.prometheus_port)

    sm = StateManager(data_dir=chunks_dir)
    channel = grpc.aio.insecure_channel(
        args.proxy,
        options=(
            ('grpc.max_send_message_length', MAX_MESSAGE_LENGTH),
            ('grpc.max_receive_message_length', MAX_MESSAGE_LENGTH),
        ),
    )
    async with channel as chan:
        transport = P2PTransport(chan, args.scheduler_id, args.logs_collector_id)
        # TODO: catch exceptions here
        worker_peer_id = await transport.initialize(logs_db_path)
        gateway_allocations = await GatewayAllocations.new(args.rpc_url, worker_peer_id, allocations_db_path)
        worker = Worker(sm, transport, args.procs)
        await monitor_service_tasks([
            asyncio.create_task(transport.run(gateway_allocations), name='p2p_transport'),
            asyncio.create_task(sm.run(), name='state_manager'),
            asyncio.create_task(worker.run(), name='worker'),
            asyncio.create_task(gateway_allocations.run(), name="gateway_allocations"),
            asyncio.create_task(run_queries(transport, worker), name='run_queries')
        ], log=LOG)


def main():
    if os.getenv('SENTRY_DSN'):
        import sentry_sdk
        sentry_sdk.init(
            traces_sample_rate=1.0
        )
    run_async_program(_main, log=LOG)
