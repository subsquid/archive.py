import sys
import time

import grpc
import msgpack

from etha.query.model import query_schema
from etha.worker.p2p.p2p_transport_pb2 import Empty, Message
from etha.worker.p2p.p2p_transport_pb2_grpc import P2PTransportStub


def run(peer_id):
    with grpc.insecure_channel('localhost:50051') as channel:
        p2p_transport = P2PTransportStub(channel)
        local_peer_id = p2p_transport.LocalPeerId(Empty()).peer_id
        print(f"Local peer ID: {local_peer_id}")

        query = query_schema.loads('{"fromBlock": 15000000, "toBlock": 15040000, "transactions": [{"to": ["0x9cb7712c6a91506e69e8751fcb08e72e1256477d"], "sighash": ["0x8ca887ca"]}], "logs": [{"address": ["0x0f98431c8ad98523631ae4a59f267346ea31f984"], "topic0": ["0x783cca1c0412dd0d695e784568c96da2e9c22ff989357a2e8b1d9b2b4e6b7118"]}, {"address": ["0xc36442b4a4522e871399cd717abdd847ab11fe88"], "topic0": ["0x3067048beee31b25b2f1681f88dac838c8bba36af25bfb2b7cf7473a5847e35f", "0x26f6a048ee9138f2c0ce266f322cb99228e8d619ae2bff30c67f8dcf9d2377b4", "0x40d0efd1a53d60ecbf40971b9daf7dc90178c3aadc7aab1765632738fa8b8f01", "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"]}, {"topic0": ["0x0c396cd989a39f4459b5fa1aed6a9a8dcdbc45908acfd67e028cd568da98982c", "0x7a53080ba414158be7ec69b987b5fb7d07dee101fe85488f0853ae16239d0bde", "0x98636036cb66a9c19a37435efc1e90142190214e8abeb821bdba3f2990dd4c95", "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67"]}], "fields": {"log": {"address": true, "topics": true, "data": true, "transaction": true}, "transaction": {"from": true, "to": true, "gasPrice": true, "gas": true}}}')
        incoming = p2p_transport.GetMessages(Empty())

        while True:
            start = time.time()

            print(f"Sending message to {peer_id}")
            msg_content: bytes = msgpack.packb(query)
            msg = Message(peer_id=peer_id, content=msg_content)
            p2p_transport.SendMessage(msg)

            print("Waiting for response")
            msg: Message = next(incoming)
            recv_query = msgpack.unpackb(msg.content)
            end = time.time()

            assert msg.peer_id == peer_id
            assert recv_query == query
            print(f"Received message from peer {msg.peer_id}. Round-trip time: {(end - start) * 1000:.3f} ms")
            time.sleep(5)


if __name__ == '__main__':
    run(sys.argv[1])