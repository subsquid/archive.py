from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Optional as _Optional

DESCRIPTOR: _descriptor.FileDescriptor

class Bytes(_message.Message):
    __slots__ = ["bytes"]
    BYTES_FIELD_NUMBER: _ClassVar[int]
    bytes: bytes
    def __init__(self, bytes: _Optional[bytes] = ...) -> None: ...

class Empty(_message.Message):
    __slots__ = []
    def __init__(self) -> None: ...

class Message(_message.Message):
    __slots__ = ["content", "peer_id", "topic"]
    CONTENT_FIELD_NUMBER: _ClassVar[int]
    PEER_ID_FIELD_NUMBER: _ClassVar[int]
    TOPIC_FIELD_NUMBER: _ClassVar[int]
    content: bytes
    peer_id: str
    topic: str
    def __init__(self, peer_id: _Optional[str] = ..., topic: _Optional[str] = ..., content: _Optional[bytes] = ...) -> None: ...

class PeerId(_message.Message):
    __slots__ = ["peer_id"]
    PEER_ID_FIELD_NUMBER: _ClassVar[int]
    peer_id: str
    def __init__(self, peer_id: _Optional[str] = ...) -> None: ...

class SignedData(_message.Message):
    __slots__ = ["data", "peer_id", "signature"]
    DATA_FIELD_NUMBER: _ClassVar[int]
    PEER_ID_FIELD_NUMBER: _ClassVar[int]
    SIGNATURE_FIELD_NUMBER: _ClassVar[int]
    data: bytes
    peer_id: str
    signature: bytes
    def __init__(self, data: _Optional[bytes] = ..., signature: _Optional[bytes] = ..., peer_id: _Optional[str] = ...) -> None: ...

class Subscription(_message.Message):
    __slots__ = ["allow_unordered", "subscribed", "topic"]
    ALLOW_UNORDERED_FIELD_NUMBER: _ClassVar[int]
    SUBSCRIBED_FIELD_NUMBER: _ClassVar[int]
    TOPIC_FIELD_NUMBER: _ClassVar[int]
    allow_unordered: bool
    subscribed: bool
    topic: str
    def __init__(self, topic: _Optional[str] = ..., subscribed: bool = ..., allow_unordered: bool = ...) -> None: ...

class VerificationResult(_message.Message):
    __slots__ = ["signature_ok"]
    SIGNATURE_OK_FIELD_NUMBER: _ClassVar[int]
    signature_ok: bool
    def __init__(self, signature_ok: bool = ...) -> None: ...
