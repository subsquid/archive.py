from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class Envelope(_message.Message):
    __slots__ = ["get_worker", "get_worker_error", "get_worker_result", "ping", "query", "query_error", "query_result", "state_update"]
    GET_WORKER_ERROR_FIELD_NUMBER: _ClassVar[int]
    GET_WORKER_FIELD_NUMBER: _ClassVar[int]
    GET_WORKER_RESULT_FIELD_NUMBER: _ClassVar[int]
    PING_FIELD_NUMBER: _ClassVar[int]
    QUERY_ERROR_FIELD_NUMBER: _ClassVar[int]
    QUERY_FIELD_NUMBER: _ClassVar[int]
    QUERY_RESULT_FIELD_NUMBER: _ClassVar[int]
    STATE_UPDATE_FIELD_NUMBER: _ClassVar[int]
    get_worker: GetWorker
    get_worker_error: QueryError
    get_worker_result: GetWorkerResult
    ping: Ping
    query: Query
    query_error: QueryError
    query_result: QueryResult
    state_update: WorkerState
    def __init__(self, ping: _Optional[_Union[Ping, _Mapping]] = ..., state_update: _Optional[_Union[WorkerState, _Mapping]] = ..., get_worker: _Optional[_Union[GetWorker, _Mapping]] = ..., get_worker_result: _Optional[_Union[GetWorkerResult, _Mapping]] = ..., get_worker_error: _Optional[_Union[QueryError, _Mapping]] = ..., query: _Optional[_Union[Query, _Mapping]] = ..., query_result: _Optional[_Union[QueryResult, _Mapping]] = ..., query_error: _Optional[_Union[QueryError, _Mapping]] = ...) -> None: ...

class GetWorker(_message.Message):
    __slots__ = ["dataset", "query_id", "start_block"]
    DATASET_FIELD_NUMBER: _ClassVar[int]
    QUERY_ID_FIELD_NUMBER: _ClassVar[int]
    START_BLOCK_FIELD_NUMBER: _ClassVar[int]
    dataset: str
    query_id: str
    start_block: int
    def __init__(self, query_id: _Optional[str] = ..., dataset: _Optional[str] = ..., start_block: _Optional[int] = ...) -> None: ...

class GetWorkerResult(_message.Message):
    __slots__ = ["encoded_dataset", "query_id", "worker_id"]
    ENCODED_DATASET_FIELD_NUMBER: _ClassVar[int]
    QUERY_ID_FIELD_NUMBER: _ClassVar[int]
    WORKER_ID_FIELD_NUMBER: _ClassVar[int]
    encoded_dataset: str
    query_id: str
    worker_id: str
    def __init__(self, query_id: _Optional[str] = ..., worker_id: _Optional[str] = ..., encoded_dataset: _Optional[str] = ...) -> None: ...

class Ping(_message.Message):
    __slots__ = ["pause", "state", "worker_id", "worker_url"]
    PAUSE_FIELD_NUMBER: _ClassVar[int]
    STATE_FIELD_NUMBER: _ClassVar[int]
    WORKER_ID_FIELD_NUMBER: _ClassVar[int]
    WORKER_URL_FIELD_NUMBER: _ClassVar[int]
    pause: bool
    state: WorkerState
    worker_id: str
    worker_url: str
    def __init__(self, worker_id: _Optional[str] = ..., worker_url: _Optional[str] = ..., state: _Optional[_Union[WorkerState, _Mapping]] = ..., pause: bool = ...) -> None: ...

class Query(_message.Message):
    __slots__ = ["dataset", "query", "query_id"]
    DATASET_FIELD_NUMBER: _ClassVar[int]
    QUERY_FIELD_NUMBER: _ClassVar[int]
    QUERY_ID_FIELD_NUMBER: _ClassVar[int]
    dataset: str
    query: str
    query_id: str
    def __init__(self, query_id: _Optional[str] = ..., dataset: _Optional[str] = ..., query: _Optional[str] = ...) -> None: ...

class QueryError(_message.Message):
    __slots__ = ["error", "query_id"]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    QUERY_ID_FIELD_NUMBER: _ClassVar[int]
    error: str
    query_id: str
    def __init__(self, query_id: _Optional[str] = ..., error: _Optional[str] = ...) -> None: ...

class QueryResult(_message.Message):
    __slots__ = ["data", "last_processed_block", "query_id", "size"]
    DATA_FIELD_NUMBER: _ClassVar[int]
    LAST_PROCESSED_BLOCK_FIELD_NUMBER: _ClassVar[int]
    QUERY_ID_FIELD_NUMBER: _ClassVar[int]
    SIZE_FIELD_NUMBER: _ClassVar[int]
    data: bytes
    last_processed_block: int
    query_id: str
    size: int
    def __init__(self, query_id: _Optional[str] = ..., last_processed_block: _Optional[int] = ..., size: _Optional[int] = ..., data: _Optional[bytes] = ...) -> None: ...

class Range(_message.Message):
    __slots__ = ["begin", "end"]
    BEGIN_FIELD_NUMBER: _ClassVar[int]
    END_FIELD_NUMBER: _ClassVar[int]
    begin: int
    end: int
    def __init__(self, begin: _Optional[int] = ..., end: _Optional[int] = ...) -> None: ...

class RangeSet(_message.Message):
    __slots__ = ["ranges"]
    RANGES_FIELD_NUMBER: _ClassVar[int]
    ranges: _containers.RepeatedCompositeFieldContainer[Range]
    def __init__(self, ranges: _Optional[_Iterable[_Union[Range, _Mapping]]] = ...) -> None: ...

class WorkerState(_message.Message):
    __slots__ = ["datasets"]
    class DatasetsEntry(_message.Message):
        __slots__ = ["key", "value"]
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: RangeSet
        def __init__(self, key: _Optional[str] = ..., value: _Optional[_Union[RangeSet, _Mapping]] = ...) -> None: ...
    DATASETS_FIELD_NUMBER: _ClassVar[int]
    datasets: _containers.MessageMap[str, RangeSet]
    def __init__(self, datasets: _Optional[_Mapping[str, RangeSet]] = ...) -> None: ...
