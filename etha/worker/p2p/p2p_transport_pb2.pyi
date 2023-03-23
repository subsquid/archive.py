from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Optional as _Optional

DESCRIPTOR: _descriptor.FileDescriptor

class Empty(_message.Message):
    __slots__ = []
    def __init__(self) -> None: ...

class Message(_message.Message):
    __slots__ = ["content", "peer_id"]
    CONTENT_FIELD_NUMBER: _ClassVar[int]
    PEER_ID_FIELD_NUMBER: _ClassVar[int]
    content: bytes
    peer_id: str
    def __init__(self, peer_id: _Optional[str] = ..., content: _Optional[bytes] = ...) -> None: ...

class PeerId(_message.Message):
    __slots__ = ["peer_id"]
    PEER_ID_FIELD_NUMBER: _ClassVar[int]
    peer_id: str
    def __init__(self, peer_id: _Optional[str] = ...) -> None: ...
