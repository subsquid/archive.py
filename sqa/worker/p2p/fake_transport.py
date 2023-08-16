import asyncio
import sys

import grpc.aio

from sqa.worker.p2p.messages_pb2 import Envelope, Query, WorkerState, RangeSet, Range
from sqa.worker.p2p.p2p_transport_pb2 import Message, Empty
from sqa.worker.p2p.p2p_transport_pb2_grpc import P2PTransportServicer, add_P2PTransportServicer_to_server


DEFAULT_STATE = WorkerState(
    datasets={
        's3://etha-mainnet': RangeSet(ranges=[
            Range(begin=16455380, end=16457279)
        ])
    }
)


class FakeP2PTransport(P2PTransportServicer):
    def __init__(self, router_id: str):
        self._router_id = router_id
        self._out_queue = asyncio.Queue(maxsize=100)

    async def _out_msg(self, envelope: Envelope) -> None:
        msg = Message(
            peer_id=self._router_id,
            content=envelope.SerializeToString()
        )
        await self._out_queue.put(msg)

    async def new_query(self, query: str, dataset: str) -> None:
        envelope = Envelope(
            query=Query(
                query_id='xxx',
                query=query,
                dataset=dataset,
            )
        )
        await self._out_msg(envelope)

    async def _update_state(self, state: WorkerState) -> None:
        envelope = Envelope(
            state_update=state
        )
        await self._out_msg(envelope)

    async def SendMessage(self, request, context):
        envelope = Envelope.FromString(request.content)
        msg_type = envelope.WhichOneof('msg')
        if msg_type == 'ping':
            print("Worker sent ping")
            await self._update_state(DEFAULT_STATE)
        elif msg_type == 'query_result':
            print(f"Query {envelope.query_result.query_id} success")
        elif msg_type == 'query_error':
            print(f"Query {envelope.query_error.query_id} error: {envelope.query_error.error}")
        else:
            print(f"Unexpected message received: {msg_type}")
        return Empty()

    async def GetMessages(self, request, context):
        while True:
            yield await self._out_queue.get()


async def read_queries(transport: FakeP2PTransport):
    loop = asyncio.get_event_loop()
    reader = asyncio.StreamReader(loop=loop)
    protocol = asyncio.StreamReaderProtocol(reader)
    await loop.connect_read_pipe(lambda: protocol, sys.stdin)

    while True:
        line = await reader.readline()
        if not line:
            print("Shutdown")
            break
        try:
            print("Sending query")
            dataset, query = line.split(maxsplit=1)
            await transport.new_query(query, dataset)
        except Exception as e:
            print(f"Query error: {e}")


async def main(router_id: str):
    transport = FakeP2PTransport(router_id)
    server = grpc.aio.server()
    add_P2PTransportServicer_to_server(transport, server)
    server.add_insecure_port("localhost:50051")
    await server.start()
    await read_queries(transport)
    await server.stop(grace=1.0)

# Example input:
# czM6Ly9ldGhhLW1haW5uZXQ {"fromBlock": 16456000, "toBlock": 16457000, "transactions": [{"to": ["0x9cb7712c6a91506e69e8751fcb08e72e1256477d"], "sighash": ["0x8ca887ca"]}], "logs": [{"address": ["0x0f98431c8ad98523631ae4a59f267346ea31f984"], "topic0": ["0x783cca1c0412dd0d695e784568c96da2e9c22ff989357a2e8b1d9b2b4e6b7118"]}, {"address": ["0xc36442b4a4522e871399cd717abdd847ab11fe88"], "topic0": ["0x3067048beee31b25b2f1681f88dac838c8bba36af25bfb2b7cf7473a5847e35f", "0x26f6a048ee9138f2c0ce266f322cb99228e8d619ae2bff30c67f8dcf9d2377b4", "0x40d0efd1a53d60ecbf40971b9daf7dc90178c3aadc7aab1765632738fa8b8f01", "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"]}, {"topic0": ["0x0c396cd989a39f4459b5fa1aed6a9a8dcdbc45908acfd67e028cd568da98982c", "0x7a53080ba414158be7ec69b987b5fb7d07dee101fe85488f0853ae16239d0bde", "0x98636036cb66a9c19a37435efc1e90142190214e8abeb821bdba3f2990dd4c95", "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67"]}], "fields": {"log": {"address": true, "topics": true, "data": true, "transaction": true}, "transaction": {"from": true, "to": true, "gasPrice": true, "gas": true}}}

if __name__ == '__main__':
    asyncio.run(main(sys.argv[1]))
