import argparse
import asyncio
import gzip
import hashlib
import logging
import multiprocessing
import os
from datetime import datetime
from json import JSONDecodeError
from typing import AsyncIterator, Optional, Dict

import grpc.aio
from marshmallow import ValidationError

from etha.query.model import query_schema
from etha.util.asyncio import create_child_task, monitor_service_tasks, run_async_program
from etha.util.child_proc import init_child_process
from etha.worker.p2p import messages_pb2 as msg_pb
from etha.worker.p2p.p2p_transport_pb2 import Message, Empty
from etha.worker.p2p.p2p_transport_pb2_grpc import P2PTransportStub
from etha.worker.query import QueryResult
from etha.worker.state.controller import State
from etha.worker.state.dataset import dataset_encode
from etha.worker.state.intervals import to_range_set
from etha.worker.state.manager import StateManager
from etha.worker.transport import Transport
from etha.worker.worker import Worker, QueryError

LOG = logging.getLogger(__name__)

# This is the maximum *decompressed* size of the message
MAX_MESSAGE_LENGTH = 100 * 1024 * 1024  # 100 MiB


def state_to_proto(state: State) -> msg_pb.WorkerState:
    return msg_pb.WorkerState(datasets={
        ds: msg_pb.RangeSet(ranges=[
            msg_pb.Range(begin=begin, end=end)
            for begin, end in range_set
        ])
        for ds, range_set in state.items()
    })


def state_from_proto(state: msg_pb.WorkerState) -> State:
    return {
        ds: to_range_set((r.begin, r.end) for r in range_set.ranges)
        for ds, range_set in state.datasets.items()
    }


def hash_and_size(data: bytes) -> msg_pb.SizeAndHash:
    hasher = hashlib.sha3_256()
    hasher.update(data)
    return msg_pb.SizeAndHash(
        size=len(data),
        sha3_256=hasher.digest()
    )


class QueryInfo:
    def __init__(self, client_id: str):
        self.client_id = client_id
        self.start_time = datetime.now()

    @property
    def exec_time_ms(self) -> int:
        return int((datetime.now() - self.start_time).total_seconds() * 1000)


class P2PTransport(Transport):
    def __init__(self, channel: grpc.aio.Channel, scheduler_id: str, send_metrics: bool = True):
        self._transport = P2PTransportStub(channel)
        self._scheduler_id = scheduler_id
        self._state_updates = asyncio.Queue(maxsize=100)
        self._query_tasks = asyncio.Queue(maxsize=100)
        self._query_info: 'Dict[str, QueryInfo]' = {}
        self._local_peer_id: 'Optional[str]' = None
        self._send_metrics = send_metrics

    async def initialize(self) -> None:
        self._local_peer_id = (await self._transport.LocalPeerId(Empty())).peer_id
        LOG.info(f"Local peer ID: {self._local_peer_id}")

    async def run(self):
        receive_task = create_child_task('p2p_receive', self._receive_loop())
        await monitor_service_tasks([receive_task], log=LOG)

    async def _receive_loop(self) -> None:
        async for msg in self._transport.GetMessages(Empty()):
            assert isinstance(msg, Message)
            envelope = msg_pb.Envelope.FromString(msg.content)
            msg_type = envelope.WhichOneof('msg')

            if msg_type == 'state_update':
                if msg.peer_id != self._scheduler_id:
                    LOG.error(f"Wrong message origin: {msg.peer_id}")
                    continue
                await self._state_updates.put(envelope.state_update)
            elif msg_type == 'query':
                self._query_info[envelope.query.query_id] = QueryInfo(msg.peer_id)
                await self._query_tasks.put(envelope.query)
            else:
                LOG.error(f"Unexpected message type received: {msg_type}")

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

    async def send_ping(self, state: State, stored_bytes: int, pause=False) -> None:
        if self._local_peer_id is None:
            await self.initialize()
        envelope = msg_pb.Envelope(ping=msg_pb.Ping(
            worker_id=self._local_peer_id,
            state=state_to_proto(state),
            stored_bytes=stored_bytes
        ))
        await self._send(envelope, peer_id=self._scheduler_id)

        for dataset, range_set in envelope.ping.state.datasets.items():
            envelope = msg_pb.Envelope(dataset_state=range_set)
            topic = dataset_encode(dataset)
            await self._send(envelope, topic=topic)

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
        exec_time_ms = query_info.exec_time_ms

        result_bytes = gzip.compress(result.result.encode())
        exec_plan = gzip.compress(result.exec_plan.encode()) if result.exec_plan is not None else None
        envelope = msg_pb.Envelope(
            query_result=msg_pb.QueryResult(
                query_id=query.query_id,
                ok=msg_pb.OkResult(
                    data=result_bytes,
                    exec_plan=exec_plan,
                )
            )
        )
        await self._send(envelope, peer_id=query_info.client_id)

        # Send execution metrics to the scheduler
        if self._send_metrics:
            envelope = msg_pb.Envelope(
                query_executed=msg_pb.QueryExecuted(
                    query=query,
                    client_id=query_info.client_id,
                    ok=msg_pb.InputAndOutput(
                        num_read_chunks=result.num_read_chunks,
                        output=hash_and_size(result_bytes),
                    ),
                    exec_time_ms=exec_time_ms,
                )
            )
            await self._send(envelope, peer_id=self._scheduler_id)

    async def send_query_error(self, query: msg_pb.Query, error: Exception) -> None:
        try:
            query_info = self._query_info.pop(query.query_id)
        except KeyError:
            LOG.error(f"Unknown query: {query.query_id}")
            return
        exec_time_ms = query_info.exec_time_ms

        if isinstance(error, (JSONDecodeError, ValidationError, QueryError)):
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

        # Send execution metrics to the scheduler
        if self._send_metrics:
            envelope = msg_pb.Envelope(
                query_executed=msg_pb.QueryExecuted(
                    query=query,
                    client_id=query_info.client_id,
                    bad_request=bad_request,
                    server_error=server_error,
                    exec_time_ms=exec_time_ms,
                )
            )
            await self._send(envelope, peer_id=self._scheduler_id)


async def run_queries(transport: P2PTransport, worker: Worker):
    async for query_task in transport.query_tasks():
        async def task():
            try:
                query = query_schema.loads(query_task.query)
                result = await worker.execute_query(query, query_task.dataset, query_task.profiling)
                LOG.info(f"Query {query_task.query_id} success")
                await transport.send_query_result(query_task, result)
            except Exception as e:
                LOG.exception(f"Query {query_task.query_id} execution failed")
                await transport.send_query_error(query_task, e)
        asyncio.create_task(task())


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
        '--no-metrics',
        action='store_true',
        help='disable sending query execution metrics to the scheduler'
    )

    args = program.parse_args()

    sm = StateManager(data_dir=args.data_dir or os.getcwd())
    channel = grpc.aio.insecure_channel(
        args.proxy,
        options=(
            ('grpc.max_send_message_length', MAX_MESSAGE_LENGTH),
            ('grpc.max_receive_message_length', MAX_MESSAGE_LENGTH),
        ),
    )
    async with channel as chan:
        transport = P2PTransport(chan, args.scheduler_id, send_metrics=(not args.no_metrics))

        with multiprocessing.Pool(processes=args.procs, initializer=init_child_process) as pool:
            worker = Worker(sm, pool, transport)

            await monitor_service_tasks([
                asyncio.create_task(transport.run(), name='p2p_transport'),
                asyncio.create_task(sm.run(), name='state_manager'),
                asyncio.create_task(worker.run(), name='worker'),
                asyncio.create_task(run_queries(transport, worker), name='run_queries')
            ], log=LOG)


def main():
    run_async_program(_main, log=LOG)
