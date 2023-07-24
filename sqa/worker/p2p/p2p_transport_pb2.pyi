from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Optional as _Optional

DESCRIPTOR: _descriptor.FileDescriptor

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

class Subscription(_message.Message):
    __slots__ = ["subscribed", "topic"]
    SUBSCRIBED_FIELD_NUMBER: _ClassVar[int]
    TOPIC_FIELD_NUMBER: _ClassVar[int]
    subscribed: bool
    topic: str
    def __init__(self, topic: _Optional[str] = ..., subscribed: bool = ...) -> None: ...
