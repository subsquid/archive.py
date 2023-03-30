import argparse
import asyncio
import logging
import multiprocessing
import os
from typing import AsyncIterator, TypedDict, Any

import aiofiles
import grpc.aio
import msgpack

from etha.util import create_child_task, monitor_service_tasks, init_child_process, run_async_program
from etha.worker.p2p.p2p_transport_pb2 import Message, Empty
from etha.worker.p2p.p2p_transport_pb2_grpc import P2PTransportStub
from etha.worker.query import QueryResult, Query
from etha.worker.state.controller import State
from etha.worker.state.manager import StateManager
from etha.worker.transport import Transport
from etha.worker.worker import Worker

LOG = logging.getLogger(__name__)


PING = 1
STATE_UPDATE = 2
QUERY = 3
QUERY_RESULT = 4
QUERY_ERROR = 5


class MsgEnvelope(TypedDict):
    msg_type: int
    msg: dict


class QueryTask(TypedDict):
    query_id: str
    data_set: str
    query: Query


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
            if msg.peer_id != self._router_id:
                LOG.error(f"Wrong message origin: {msg.peer_id}")
                continue
            envelope: MsgEnvelope = msgpack.unpackb(msg.content)

            if envelope['msg_type'] == STATE_UPDATE:
                await self._state_updates.put(envelope['msg'])
            elif envelope['msg_type'] == QUERY:
                await self._query_tasks.put(envelope['msg'])
            else:
                LOG.error(f"Unexpected message type received: {envelope['msg_type']}")

    async def _send(self, msg_content: Any) -> None:
        msg = Message(
            peer_id=self._router_id,
            content=msgpack.packb(msg_content)
        )
        await self._transport.SendMessage(msg)

    async def send_ping(self, state: State, pause=False) -> None:
        envelope: MsgEnvelope = {
            'msg_type': PING,
            'msg': state
        }
        await self._send(envelope)

    async def state_updates(self) -> AsyncIterator[State]:
        while True:
            yield await self._state_updates.get()

    async def query_tasks(self) -> AsyncIterator[QueryTask]:
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

        envelope: MsgEnvelope = {
            'msg_type': QUERY_RESULT,
            'msg': {
                'query_id': query_id,
                'last_processed_block': result.last_processed_block,
                'size': result.size,
                'data': data
            }
        }
        await self._send(envelope)

    async def send_query_error(self, query_id: str, error: Exception) -> None:
        envelope: MsgEnvelope = {
            'msg_type': QUERY_ERROR,
            'msg': {
                'query_id': query_id,
                'error': str(error),
            }
        }
        await self._send(envelope)


async def run_queries(transport: P2PTransport, worker: Worker):
    async for query_task in transport.query_tasks():
        async def task():
            try:
                result = await worker.execute_query(query_task['query'], query_task['data_set'])
                LOG.info(f"Query {query_task['query_id']} success")
                await transport.send_query_result(query_task['query_id'], result)
            except Exception as e:
                LOG.exception(f"Query {query_task['query_id']} execution failed")
                await transport.send_query_error(query_task['query_id'], e)
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
                asyncio.create_task(sm.run(), name='state_manager'),
                asyncio.create_task(worker.run(), name='worker'),
                asyncio.create_task(run_queries(transport, worker), name='run_queries')
            ], log=LOG)


if __name__ == '__main__':
    run_async_program(main(), log=LOG)
