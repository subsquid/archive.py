import argparse
import asyncio
import json
import logging
import os
from json import JSONDecodeError
from typing import AsyncIterator, Optional, Dict

import grpc.aio
from marshmallow import ValidationError

from sqa.gateway_controller import Gateways
from sqa.util.asyncio import create_child_task, monitor_service_tasks, run_async_program
from sqa.worker.p2p import messages_pb2 as msg_pb
from sqa.worker.p2p.p2p_transport_pb2 import Bytes, Empty, Message, Subscription, SignedData
from sqa.worker.p2p.p2p_transport_pb2_grpc import P2PTransportStub
from sqa.worker.p2p.query_logs import LogsStorage
from sqa.worker.p2p.util import state_to_proto, sha3_256, state_from_proto, QueryInfo
from sqa.worker.query import QueryResult, InvalidQuery
from sqa.worker.state.controller import State
from sqa.worker.state.dataset import dataset_decode
from sqa.worker.state.manager import StateManager
from sqa.worker.worker import Worker


LOG = logging.getLogger(__name__)

# This is the maximum *decompressed* size of the message
MAX_MESSAGE_LENGTH = 100 * 1024 * 1024  # 100 MiB
LOGS_SEND_INTERVAL_SEC = int(os.environ.get('LOGS_SEND_INTERVAL_SEC', '600'))
PING_TOPIC = "worker_ping"
LOGS_TOPIC = "worker_query_logs"
WORKER_VERSION = "0.1.5"


class P2PTransport:
    def __init__(self, channel: grpc.aio.Channel, scheduler_id: str, logs_collector_id: str):
        self._transport = P2PTransportStub(channel)
        self._scheduler_id = scheduler_id
        self._logs_collector_id = logs_collector_id
        self._state_updates = asyncio.Queue(maxsize=100)
        self._query_tasks = asyncio.Queue(maxsize=100)
        self._query_info: 'Dict[str, QueryInfo]' = {}
        self._logs_storage: 'Optional[LogsStorage]' = None
        self._local_peer_id: 'Optional[str]' = None
        self._expected_pong: 'Optional[bytes]' = None  # Hash of the last sent ping

    async def initialize(self, logs_db_path: str) -> str:
        self._local_peer_id = (await self._transport.LocalPeerId(Empty())).peer_id
        LOG.info(f"Local peer ID: {self._local_peer_id}")
        self._logs_storage = LogsStorage(self._local_peer_id, logs_db_path)

        # Subscribe to the ping & logs topics so that our node also participates in broadcasting messages
        await self._subscribe(topic=PING_TOPIC)
        await self._subscribe(topic=LOGS_TOPIC)

        return self._local_peer_id

    async def run(self):
        receive_task = create_child_task('p2p_receive', self._receive_loop())
        send_logs_task = create_child_task('p2p_send_logs', self._send_logs_loop())
        await monitor_service_tasks([receive_task, send_logs_task], log=LOG)

    async def _receive_loop(self) -> None:
        async for msg in self._transport.GetMessages(Empty()):
            assert isinstance(msg, Message)
            envelope = msg_pb.Envelope.FromString(msg.content)
            msg_type = envelope.WhichOneof('msg')

            if msg_type == 'pong':
                await self._handle_pong(msg.peer_id, envelope.pong)

            elif msg_type == 'query':
                await self._handle_query(msg.peer_id, envelope.query)

            elif msg_type == 'logs_collected':
                if msg.peer_id != self._logs_collector_id:
                    LOG.warning(f"Wrong next_seq_no message origin: {msg.peer_id}")
                    continue
                last_collected_seq_no = envelope.logs_collected.sequence_numbers.get(self._local_peer_id)
                self._logs_storage.logs_collected(last_collected_seq_no)

            elif msg_type in ('ping', 'query_logs'):
                continue  # Just ignore pings and logs from other workers

            else:
                LOG.warning(f"Unexpected message type received: {msg_type}")

    async def _send_logs_loop(self) -> None:
        while True:
            await asyncio.sleep(LOGS_SEND_INTERVAL_SEC)
            if not self._logs_storage.is_initialized:
                continue

            # Sign all the logs
            query_logs = self._logs_storage.get_logs()
            for log in query_logs:
                signature: Bytes = await self._transport.Sign(Bytes(bytes=log.SerializeToString()))
                log.signature = signature.bytes

            # Send out
            envelope = msg_pb.Envelope(query_logs=msg_pb.QueryLogs(queries_executed=query_logs))
            LOG.debug("Sending out query logs")
            await self._send(envelope, topic=LOGS_TOPIC)

    async def _handle_pong(self, peer_id: str, msg: msg_pb.Pong) -> None:
        if peer_id != self._scheduler_id:
            LOG.warning(f"Wrong pong message origin: {peer_id}")
            return
        if msg.ping_hash != self._expected_pong:
            LOG.warning("Invalid ping hash in received pong message")
            return
        self._expected_pong = None

        status = msg.WhichOneof('status')
        if status == 'not_registered':
            LOG.error("Worker not registered on chain")
        elif status == 'unsupported_version':
            LOG.error("Worker version not supported by the scheduler")
        elif status == 'jailed':
            LOG.info("Worker jailed until the end of epoch")
        elif status == 'active':
            # If the status is 'active', it contains assigned chunks
            await self._state_updates.put(msg.active)

    async def _handle_query(self, peer_id: str, query: msg_pb.Query) -> None:
        if not self._logs_storage.is_initialized:
            LOG.warning("Logs storage not initialized. Cannot execute queries yet.")
            return

        # Verify query signature
        signature = query.signature
        query.signature = b""
        signed_data = SignedData(
            data=query.SerializeToString(),
            signature=signature,
            peer_id=peer_id
        )
        verification_result = await self._transport.VerifySignature(signed_data)
        if not verification_result.signature_ok:
            LOG.warning(f"Query with invalid signature received from {peer_id}")
            return
        query.signature = signature
        query.client_id = peer_id

        self._query_info[query.query_id] = QueryInfo(peer_id)
        await self._query_tasks.put(query)

    async def _send(
            self,
            envelope: msg_pb.Envelope,
            peer_id: Optional[str] = None,
            topic: Optional[str] = None,
    ) -> None:
        msg = Message(
            peer_id=peer_id,
            topic=topic,
            content=envelope.SerializeToString()
        )
        await self._transport.SendMessage(msg)

    async def _subscribe(self, topic: str) -> None:
        await self._transport.ToggleSubscription(Subscription(topic=topic, subscribed=True))

    async def send_ping(self, state: State, stored_bytes: int, pause=False) -> None:
        ping = msg_pb.Ping(
            worker_id=self._local_peer_id,
            state=state_to_proto(state),
            stored_bytes=stored_bytes,
            version=WORKER_VERSION,
        )
        signature: Bytes = await self._transport.Sign(Bytes(bytes=ping.SerializeToString()))
        ping.signature = signature.bytes
        envelope = msg_pb.Envelope(ping=ping)

        # We expect pong to be delivered before the next ping is sent
        if self._expected_pong is not None:
            LOG.error("Pong message not received in time. The scheduler is not responding. Contact tech support.")
        self._expected_pong = sha3_256(ping.SerializeToString())

        await self._send(envelope, topic=PING_TOPIC)

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

        envelope = msg_pb.Envelope(
            query_result=msg_pb.QueryResult(
                query_id=query.query_id,
                ok=msg_pb.OkResult(
                    data=result.gzipped_bytes,
                    exec_plan=None,
                )
            )
        )
        await self._send(envelope, peer_id=query_info.client_id)
        self._logs_storage.query_executed(query, query_info, result)

    async def send_query_error(self, query: msg_pb.Query, error: Exception) -> None:
        try:
            query_info = self._query_info.pop(query.query_id)
        except KeyError:
            LOG.error(f"Unknown query: {query.query_id}")
            return
        query_info.finished()

        if isinstance(error, (JSONDecodeError, ValidationError, InvalidQuery)):
            bad_request, server_error = str(error), None
        else:
            bad_request, server_error = None, str(error)

        envelope = msg_pb.Envelope(
            query_result=msg_pb.QueryResult(
                query_id=query.query_id,
                bad_request=bad_request,
                server_error=server_error,
            )
        )
        await self._send(envelope, peer_id=query_info.client_id)
        self._logs_storage.query_error(query, query_info, bad_request, server_error)


async def execute_query(transport: P2PTransport, worker: Worker, query_task: msg_pb.Query):
    try:
        query = json.loads(query_task.query)
        dataset = dataset_decode(query_task.dataset)
        result = await worker.execute_query(query, dataset, query_task.client_id, query_task.profiling)
        LOG.info(f"Query {query_task.query_id} success")
        await transport.send_query_result(query_task, result)
    except Exception as e:
        LOG.warning(f"Query {query_task.query_id} execution failed")
        await transport.send_query_error(query_task, e)


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
        '--logs-db',
        metavar='PATH',
        help='path to the logs DB file (defaults to ./logs.db)'
    )
    program.add_argument(
        '--procs',
        type=int,
        help='number of processes to use to execute data queries'
    )
    args = program.parse_args()

    data_dir = args.data_dir or os.getcwd()
    sm = StateManager(data_dir=data_dir)
    channel = grpc.aio.insecure_channel(
        args.proxy,
        options=(
            ('grpc.max_send_message_length', MAX_MESSAGE_LENGTH),
            ('grpc.max_receive_message_length', MAX_MESSAGE_LENGTH),
        ),
    )
    async with channel as chan:
        transport = P2PTransport(chan, args.scheduler_id, args.logs_collector_id)
        peer_id = await transport.initialize(args.logs_db or os.path.join(os.getcwd(), 'logs.db'))

        worker = Worker(sm, transport, Gateways(peer_id, data_dir), args.procs)

        await monitor_service_tasks([
            asyncio.create_task(transport.run(), name='p2p_transport'),
            asyncio.create_task(sm.run(), name='state_manager'),
            asyncio.create_task(worker.run(), name='worker'),
            asyncio.create_task(run_queries(transport, worker), name='run_queries')
        ], log=LOG)


def main():
    if os.getenv('SENTRY_DSN'):
        import sentry_sdk
        sentry_sdk.init(
            traces_sample_rate=1.0
        )
    run_async_program(_main, log=LOG)
