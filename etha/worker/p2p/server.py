import argparse
import asyncio
import logging
import multiprocessing
import os
from typing import AsyncIterator

import aiofiles
import grpc.aio

from etha.query.model import query_schema
from etha.util import create_child_task, monitor_service_tasks, init_child_process, run_async_program
from etha.worker.p2p import messages_pb2 as msg_pb
from etha.worker.p2p.p2p_transport_pb2 import Message, Empty
from etha.worker.p2p.p2p_transport_pb2_grpc import P2PTransportStub
from etha.worker.query import QueryResult
from etha.worker.state.controller import State
from etha.worker.state.intervals import to_range_set
from etha.worker.state.manager import StateManager
from etha.worker.transport import Transport
from etha.worker.worker import Worker

LOG = logging.getLogger(__name__)


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


class P2PTransport(Transport):
    def __init__(self, channel: grpc.aio.Channel, router_id: str):
        self._transport = P2PTransportStub(channel)
        self._router_id = router_id
        self._state_updates = asyncio.Queue(maxsize=100)
        self._query_tasks = asyncio.Queue(maxsize=100)

    async def run(self):
        receive_task = create_child_task('p2p_receive', self._receive_loop())
        await monitor_service_tasks([receive_task], log=LOG)

    async def _receive_loop(self) -> None:
        async for msg in self._transport.GetMessages(Empty()):
            assert isinstance(msg, Message)
            if msg.peer_id != self._router_id:
                LOG.error(f"Wrong message origin: {msg.peer_id}")
                continue
            envelope = msg_pb.Envelope.FromString(msg.content)
            msg_type = envelope.WhichOneof('msg')

            if msg_type == 'state_update':
                await self._state_updates.put(envelope.state_update)
            elif msg_type == 'query':
                await self._query_tasks.put(envelope.query)
            else:
                LOG.error(f"Unexpected message type received: {msg_type}")

    async def _send(self, envelope: msg_pb.Envelope) -> None:
        msg = Message(
            peer_id=self._router_id,
            content=envelope.SerializeToString()
        )
        await self._transport.SendMessage(msg)

    async def send_ping(self, state: State, pause=False) -> None:
        envelope = msg_pb.Envelope(ping=msg_pb.Ping(state=state_to_proto(state)))
        await self._send(envelope)

    async def state_updates(self) -> AsyncIterator[State]:
        while True:
            state = await self._state_updates.get()
            yield state_from_proto(state)

    async def query_tasks(self) -> AsyncIterator[msg_pb.Query]:
        while True:
            yield await self._query_tasks.get()

    async def send_query_result(self, query_id: str, result: QueryResult) -> None:
        if isinstance(result.data, bytes):
            data = result.data
        elif isinstance(result.data, str):
            async with aiofiles.open(result.data, 'rb') as f:
                data = f.read()
            await aiofiles.os.unlink(result.data)
        else:
            data = None

        envelope = msg_pb.Envelope(
            query_result=msg_pb.QueryResult(
                query_id=query_id,
                last_processed_block=result.last_processed_block,
                size=result.size,
                data=data
            )
        )
        await self._send(envelope)

    async def send_query_error(self, query_id: str, error: Exception) -> None:
        envelope = msg_pb.Envelope(
            query_error=msg_pb.QueryError(
                query_id=query_id,
                error=str(error)
            )
        )
        await self._send(envelope)


async def run_queries(transport: P2PTransport, worker: Worker):
    async for query_task in transport.query_tasks():
        async def task():
            try:
                query = query_schema.loads(query_task.query)
                result = await worker.execute_query(query, query_task.dataset)
                LOG.info(f"Query {query_task.query_id} success")
                await transport.send_query_result(query_task.query_id, result)
            except Exception as e:
                LOG.exception(f"Query {query_task.query_id} execution failed")
                await transport.send_query_error(query_task.query_id, e)
        asyncio.create_task(task())


async def main():
    program = argparse.ArgumentParser(
        description='Subsquid eth archive worker'
    )
    program.add_argument(
        '--proxy',
        metavar='URL',
        default='localhost:50051',
        help='URL of the P2P proxy service to connect to'
    )
    program.add_argument(
        '--router-id',
        required=True,
        help='Peer ID of the router'
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

    args = program.parse_args()

    sm = StateManager(data_dir=args.data_dir or os.getcwd())
    async with grpc.aio.insecure_channel(args.proxy) as channel:
        transport = P2PTransport(channel, args.router_id)

        with multiprocessing.Pool(processes=args.procs, initializer=init_child_process) as pool:
            worker = Worker(sm, pool, transport)

            await monitor_service_tasks([
                asyncio.create_task(transport.run(), name='p2p_transport'),
                asyncio.create_task(sm.run(), name='state_manager'),
                asyncio.create_task(worker.run(), name='worker'),
                asyncio.create_task(run_queries(transport, worker), name='run_queries')
            ], log=LOG)


if __name__ == '__main__':
    run_async_program(main(), log=LOG)
