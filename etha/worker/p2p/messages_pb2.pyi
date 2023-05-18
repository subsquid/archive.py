from google.protobuf import empty_pb2 as _empty_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class Envelope(_message.Message):
    __slots__ = ["dataset_state", "get_worker", "get_worker_result", "ping", "query", "query_executed", "query_finished", "query_result", "query_submitted", "state_update"]
    DATASET_STATE_FIELD_NUMBER: _ClassVar[int]
    GET_WORKER_FIELD_NUMBER: _ClassVar[int]
    GET_WORKER_RESULT_FIELD_NUMBER: _ClassVar[int]
    PING_FIELD_NUMBER: _ClassVar[int]
    QUERY_EXECUTED_FIELD_NUMBER: _ClassVar[int]
    QUERY_FIELD_NUMBER: _ClassVar[int]
    QUERY_FINISHED_FIELD_NUMBER: _ClassVar[int]
    QUERY_RESULT_FIELD_NUMBER: _ClassVar[int]
    QUERY_SUBMITTED_FIELD_NUMBER: _ClassVar[int]
    STATE_UPDATE_FIELD_NUMBER: _ClassVar[int]
    dataset_state: RangeSet
    get_worker: GetWorker
    get_worker_result: GetWorkerResult
    ping: Ping
    query: Query
    query_executed: QueryExecuted
    query_finished: QueryFinished
    query_result: QueryResult
    query_submitted: QuerySubmitted
    state_update: WorkerState
    def __init__(self, ping: _Optional[_Union[Ping, _Mapping]] = ..., state_update: _Optional[_Union[WorkerState, _Mapping]] = ..., get_worker: _Optional[_Union[GetWorker, _Mapping]] = ..., get_worker_result: _Optional[_Union[GetWorkerResult, _Mapping]] = ..., query: _Optional[_Union[Query, _Mapping]] = ..., query_result: _Optional[_Union[QueryResult, _Mapping]] = ..., dataset_state: _Optional[_Union[RangeSet, _Mapping]] = ..., query_submitted: _Optional[_Union[QuerySubmitted, _Mapping]] = ..., query_finished: _Optional[_Union[QueryFinished, _Mapping]] = ..., query_executed: _Optional[_Union[QueryExecuted, _Mapping]] = ...) -> None: ...

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
    __slots__ = ["error", "query_id", "worker_id"]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    QUERY_ID_FIELD_NUMBER: _ClassVar[int]
    WORKER_ID_FIELD_NUMBER: _ClassVar[int]
    error: str
    query_id: str
    worker_id: str
    def __init__(self, query_id: _Optional[str] = ..., worker_id: _Optional[str] = ..., error: _Optional[str] = ...) -> None: ...

class InputAndOutput(_message.Message):
    __slots__ = ["input", "output"]
    INPUT_FIELD_NUMBER: _ClassVar[int]
    OUTPUT_FIELD_NUMBER: _ClassVar[int]
    input: SizeAndHash
    output: SizeAndHash
    def __init__(self, input: _Optional[_Union[SizeAndHash, _Mapping]] = ..., output: _Optional[_Union[SizeAndHash, _Mapping]] = ...) -> None: ...

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

class QueryExecuted(_message.Message):
    __slots__ = ["bad_request", "client_id", "exec_time_ms", "ok", "query", "server_error"]
    BAD_REQUEST_FIELD_NUMBER: _ClassVar[int]
    CLIENT_ID_FIELD_NUMBER: _ClassVar[int]
    EXEC_TIME_MS_FIELD_NUMBER: _ClassVar[int]
    OK_FIELD_NUMBER: _ClassVar[int]
    QUERY_FIELD_NUMBER: _ClassVar[int]
    SERVER_ERROR_FIELD_NUMBER: _ClassVar[int]
    bad_request: str
    client_id: str
    exec_time_ms: int
    ok: InputAndOutput
    query: Query
    server_error: str
    def __init__(self, query: _Optional[_Union[Query, _Mapping]] = ..., client_id: _Optional[str] = ..., exec_time_ms: _Optional[int] = ..., ok: _Optional[_Union[InputAndOutput, _Mapping]] = ..., bad_request: _Optional[str] = ..., server_error: _Optional[str] = ...) -> None: ...

class QueryFinished(_message.Message):
    __slots__ = ["bad_request", "exec_time_ms", "ok", "query_id", "server_error", "timeout", "worker_id"]
    BAD_REQUEST_FIELD_NUMBER: _ClassVar[int]
    EXEC_TIME_MS_FIELD_NUMBER: _ClassVar[int]
    OK_FIELD_NUMBER: _ClassVar[int]
    QUERY_ID_FIELD_NUMBER: _ClassVar[int]
    SERVER_ERROR_FIELD_NUMBER: _ClassVar[int]
    TIMEOUT_FIELD_NUMBER: _ClassVar[int]
    WORKER_ID_FIELD_NUMBER: _ClassVar[int]
    bad_request: str
    exec_time_ms: int
    ok: SizeAndHash
    query_id: str
    server_error: str
    timeout: _empty_pb2.Empty
    worker_id: str
    def __init__(self, query_id: _Optional[str] = ..., worker_id: _Optional[str] = ..., exec_time_ms: _Optional[int] = ..., ok: _Optional[_Union[SizeAndHash, _Mapping]] = ..., bad_request: _Optional[str] = ..., server_error: _Optional[str] = ..., timeout: _Optional[_Union[_empty_pb2.Empty, _Mapping]] = ...) -> None: ...

class QueryResult(_message.Message):
    __slots__ = ["bad_request", "ok_data", "query_id", "server_error"]
    BAD_REQUEST_FIELD_NUMBER: _ClassVar[int]
    OK_DATA_FIELD_NUMBER: _ClassVar[int]
    QUERY_ID_FIELD_NUMBER: _ClassVar[int]
    SERVER_ERROR_FIELD_NUMBER: _ClassVar[int]
    bad_request: str
    ok_data: bytes
    query_id: str
    server_error: str
    def __init__(self, query_id: _Optional[str] = ..., ok_data: _Optional[bytes] = ..., bad_request: _Optional[str] = ..., server_error: _Optional[str] = ...) -> None: ...

class QuerySubmitted(_message.Message):
    __slots__ = ["query", "worker_id"]
    QUERY_FIELD_NUMBER: _ClassVar[int]
    WORKER_ID_FIELD_NUMBER: _ClassVar[int]
    query: Query
    worker_id: str
    def __init__(self, query: _Optional[_Union[Query, _Mapping]] = ..., worker_id: _Optional[str] = ...) -> None: ...

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

class SizeAndHash(_message.Message):
    __slots__ = ["sha3_256", "size"]
    SHA3_256_FIELD_NUMBER: _ClassVar[int]
    SIZE_FIELD_NUMBER: _ClassVar[int]
    sha3_256: bytes
    size: int
    def __init__(self, size: _Optional[int] = ..., sha3_256: _Optional[bytes] = ...) -> None: ...

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
