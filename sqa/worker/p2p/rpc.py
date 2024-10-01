import asyncio
import functools
import logging
from typing import AsyncIterator, Optional

import google.protobuf.message
import grpc.aio

from sqa.worker.p2p import messages_pb2 as msg_pb
from sqa.worker.p2p.p2p_transport_pb2 import Bytes, Empty, Message, Subscription, SignedData, VerificationResult, PeerId
from sqa.worker.p2p.p2p_transport_pb2_grpc import P2PTransportStub

LOG = logging.getLogger(__name__)

MAX_RETRIES = 5
INIT_BACKOFF = 0.5  # seconds
BACKOFF_FACTOR = 2.0


def retry(f):
    @functools.wraps(f)
    async def wrapped(self, *args, **kwargs):
        backoff = self.init_backoff
        for _ in range(self.max_retries):
            try:
                return await f(self, *args, **kwargs)
            except grpc.RpcError:
                LOG.warning(f"RPC error. Connection re-try in {backoff} seconds.")
                await asyncio.sleep(backoff)
                backoff *= BACKOFF_FACTOR
        return await f(self, *args, **kwargs)
    return wrapped


def retry_stream(f):
    @functools.wraps(f)
    async def wrapped(self, *args, **kwargs):
        retries = 0
        while retries <= self.max_retries:
            try:
                async for x in f(self, *args, **kwargs):
                    retries = 0
                    yield x
            except grpc.RpcError:
                backoff = self.init_backoff * (BACKOFF_FACTOR ** retries)
                LOG.warning(f"RPC error. Connection re-try in {backoff} seconds.")
                await asyncio.sleep(backoff)
                retries += 1
    return wrapped


class RPCWrapper:
    def __init__(
            self,
            channel: grpc.aio.Channel,
            max_retries: int = MAX_RETRIES,
            init_backoff: float = INIT_BACKOFF
    ):
        self._transport = P2PTransportStub(channel)
        self.max_retries = max_retries
        self.init_backoff = init_backoff

    @retry
    async def local_peer_id(self) -> str:
        peer_id: PeerId = await self._transport.LocalPeerId(Empty())
        return peer_id.peer_id

    @retry
    async def subscribe(self, topic: str) -> None:
        subscription = Subscription(topic=topic, subscribed=True)
        await self._transport.ToggleSubscription(subscription)

    @retry_stream
    async def get_messages(self) -> AsyncIterator[tuple[str, msg_pb.Envelope]]:
        async for msg in self._transport.GetMessages(Empty()):
            assert isinstance(msg, Message)
            try:
                envelope = msg_pb.Envelope.FromString(msg.content)
                yield msg.peer_id, envelope
            except (AttributeError, ValueError, google.protobuf.message.DecodeError):
                LOG.warning(f"Invalid message received from {msg.peer_id}")

    @retry
    async def send_msg(
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

    @retry
    async def sign_msg(self, msg: google.protobuf.message.Message) -> None:
        msg_bytes = Bytes(bytes=msg.SerializeToString())
        signature: Bytes = await self._transport.Sign(msg_bytes)
        msg.signature = signature.bytes  # noqa

    @retry
    async def verify_signature(
            self,
            data: bytes,
            signature: bytes,
            peer_id: str
    ) -> bool:
        signed_data = SignedData(
            data=data,
            signature=signature,
            peer_id=peer_id,
        )
        result: VerificationResult = await self._transport.VerifySignature(signed_data)
        return result.signature_ok

