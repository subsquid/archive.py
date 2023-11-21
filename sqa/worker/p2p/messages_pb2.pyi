from google.protobuf import empty_pb2 as _empty_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class Envelope(_message.Message):
    __slots__ = ["dataset_state", "logs_collected", "ping", "pong", "query", "query_finished", "query_logs", "query_result", "query_submitted"]
    DATASET_STATE_FIELD_NUMBER: _ClassVar[int]
    LOGS_COLLECTED_FIELD_NUMBER: _ClassVar[int]
    PING_FIELD_NUMBER: _ClassVar[int]
    PONG_FIELD_NUMBER: _ClassVar[int]
    QUERY_FIELD_NUMBER: _ClassVar[int]
    QUERY_FINISHED_FIELD_NUMBER: _ClassVar[int]
    QUERY_LOGS_FIELD_NUMBER: _ClassVar[int]
    QUERY_RESULT_FIELD_NUMBER: _ClassVar[int]
    QUERY_SUBMITTED_FIELD_NUMBER: _ClassVar[int]
    dataset_state: RangeSet
    logs_collected: LogsCollected
    ping: Ping
    pong: Pong
    query: Query
    query_finished: QueryFinished
    query_logs: QueryLogs
    query_result: QueryResult
    query_submitted: QuerySubmitted
    def __init__(self, ping: _Optional[_Union[Ping, _Mapping]] = ..., pong: _Optional[_Union[Pong, _Mapping]] = ..., query: _Optional[_Union[Query, _Mapping]] = ..., query_result: _Optional[_Union[QueryResult, _Mapping]] = ..., dataset_state: _Optional[_Union[RangeSet, _Mapping]] = ..., query_submitted: _Optional[_Union[QuerySubmitted, _Mapping]] = ..., query_finished: _Optional[_Union[QueryFinished, _Mapping]] = ..., query_logs: _Optional[_Union[QueryLogs, _Mapping]] = ..., logs_collected: _Optional[_Union[LogsCollected, _Mapping]] = ...) -> None: ...

class InputAndOutput(_message.Message):
    __slots__ = ["num_read_chunks", "output"]
    NUM_READ_CHUNKS_FIELD_NUMBER: _ClassVar[int]
    OUTPUT_FIELD_NUMBER: _ClassVar[int]
    num_read_chunks: int
    output: SizeAndHash
    def __init__(self, num_read_chunks: _Optional[int] = ..., output: _Optional[_Union[SizeAndHash, _Mapping]] = ...) -> None: ...

class LogsCollected(_message.Message):
    __slots__ = ["sequence_numbers"]
    class SequenceNumbersEntry(_message.Message):
        __slots__ = ["key", "value"]
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: int
        def __init__(self, key: _Optional[str] = ..., value: _Optional[int] = ...) -> None: ...
    SEQUENCE_NUMBERS_FIELD_NUMBER: _ClassVar[int]
    sequence_numbers: _containers.ScalarMap[str, int]
    def __init__(self, sequence_numbers: _Optional[_Mapping[str, int]] = ...) -> None: ...

class OkResult(_message.Message):
    __slots__ = ["data", "exec_plan"]
    DATA_FIELD_NUMBER: _ClassVar[int]
    EXEC_PLAN_FIELD_NUMBER: _ClassVar[int]
    data: bytes
    exec_plan: bytes
    def __init__(self, data: _Optional[bytes] = ..., exec_plan: _Optional[bytes] = ...) -> None: ...

class Ping(_message.Message):
    __slots__ = ["pause", "signature", "state", "stored_bytes", "version", "worker_id", "worker_url"]
    PAUSE_FIELD_NUMBER: _ClassVar[int]
    SIGNATURE_FIELD_NUMBER: _ClassVar[int]
    STATE_FIELD_NUMBER: _ClassVar[int]
    STORED_BYTES_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    WORKER_ID_FIELD_NUMBER: _ClassVar[int]
    WORKER_URL_FIELD_NUMBER: _ClassVar[int]
    pause: bool
    signature: bytes
    state: WorkerState
    stored_bytes: int
    version: str
    worker_id: str
    worker_url: str
    def __init__(self, worker_id: _Optional[str] = ..., worker_url: _Optional[str] = ..., state: _Optional[_Union[WorkerState, _Mapping]] = ..., pause: bool = ..., stored_bytes: _Optional[int] = ..., version: _Optional[str] = ..., signature: _Optional[bytes] = ...) -> None: ...

class Pong(_message.Message):
    __slots__ = ["active", "jailed", "not_registered", "ping_hash", "unsupported_version"]
    ACTIVE_FIELD_NUMBER: _ClassVar[int]
    JAILED_FIELD_NUMBER: _ClassVar[int]
    NOT_REGISTERED_FIELD_NUMBER: _ClassVar[int]
    PING_HASH_FIELD_NUMBER: _ClassVar[int]
    UNSUPPORTED_VERSION_FIELD_NUMBER: _ClassVar[int]
    active: WorkerState
    jailed: _empty_pb2.Empty
    not_registered: _empty_pb2.Empty
    ping_hash: bytes
    unsupported_version: _empty_pb2.Empty
    def __init__(self, ping_hash: _Optional[bytes] = ..., not_registered: _Optional[_Union[_empty_pb2.Empty, _Mapping]] = ..., unsupported_version: _Optional[_Union[_empty_pb2.Empty, _Mapping]] = ..., jailed: _Optional[_Union[_empty_pb2.Empty, _Mapping]] = ..., active: _Optional[_Union[WorkerState, _Mapping]] = ...) -> None: ...

class Query(_message.Message):
    __slots__ = ["dataset", "profiling", "query", "query_id", "signature"]
    DATASET_FIELD_NUMBER: _ClassVar[int]
    PROFILING_FIELD_NUMBER: _ClassVar[int]
    QUERY_FIELD_NUMBER: _ClassVar[int]
    QUERY_ID_FIELD_NUMBER: _ClassVar[int]
    SIGNATURE_FIELD_NUMBER: _ClassVar[int]
    dataset: str
    profiling: bool
    query: str
    query_id: str
    signature: bytes
    def __init__(self, query_id: _Optional[str] = ..., dataset: _Optional[str] = ..., query: _Optional[str] = ..., profiling: bool = ..., signature: _Optional[bytes] = ...) -> None: ...

class QueryExecuted(_message.Message):
    __slots__ = ["bad_request", "client_id", "exec_time_ms", "ok", "query", "query_hash", "seq_no", "server_error", "worker_id"]
    BAD_REQUEST_FIELD_NUMBER: _ClassVar[int]
    CLIENT_ID_FIELD_NUMBER: _ClassVar[int]
    EXEC_TIME_MS_FIELD_NUMBER: _ClassVar[int]
    OK_FIELD_NUMBER: _ClassVar[int]
    QUERY_FIELD_NUMBER: _ClassVar[int]
    QUERY_HASH_FIELD_NUMBER: _ClassVar[int]
    SEQ_NO_FIELD_NUMBER: _ClassVar[int]
    SERVER_ERROR_FIELD_NUMBER: _ClassVar[int]
    WORKER_ID_FIELD_NUMBER: _ClassVar[int]
    bad_request: str
    client_id: str
    exec_time_ms: int
    ok: InputAndOutput
    query: Query
    query_hash: bytes
    seq_no: int
    server_error: str
    worker_id: str
    def __init__(self, client_id: _Optional[str] = ..., worker_id: _Optional[str] = ..., query: _Optional[_Union[Query, _Mapping]] = ..., query_hash: _Optional[bytes] = ..., exec_time_ms: _Optional[int] = ..., ok: _Optional[_Union[InputAndOutput, _Mapping]] = ..., bad_request: _Optional[str] = ..., server_error: _Optional[str] = ..., seq_no: _Optional[int] = ...) -> None: ...

class QueryFinished(_message.Message):
    __slots__ = ["bad_request", "client_id", "exec_time_ms", "ok", "query_id", "server_error", "timeout", "worker_id"]
    BAD_REQUEST_FIELD_NUMBER: _ClassVar[int]
    CLIENT_ID_FIELD_NUMBER: _ClassVar[int]
    EXEC_TIME_MS_FIELD_NUMBER: _ClassVar[int]
    OK_FIELD_NUMBER: _ClassVar[int]
    QUERY_ID_FIELD_NUMBER: _ClassVar[int]
    SERVER_ERROR_FIELD_NUMBER: _ClassVar[int]
    TIMEOUT_FIELD_NUMBER: _ClassVar[int]
    WORKER_ID_FIELD_NUMBER: _ClassVar[int]
    bad_request: str
    client_id: str
    exec_time_ms: int
    ok: SizeAndHash
    query_id: str
    server_error: str
    timeout: _empty_pb2.Empty
    worker_id: str
    def __init__(self, client_id: _Optional[str] = ..., worker_id: _Optional[str] = ..., query_id: _Optional[str] = ..., exec_time_ms: _Optional[int] = ..., ok: _Optional[_Union[SizeAndHash, _Mapping]] = ..., bad_request: _Optional[str] = ..., server_error: _Optional[str] = ..., timeout: _Optional[_Union[_empty_pb2.Empty, _Mapping]] = ...) -> None: ...

class QueryLogs(_message.Message):
    __slots__ = ["queries_executed", "signature"]
    QUERIES_EXECUTED_FIELD_NUMBER: _ClassVar[int]
    SIGNATURE_FIELD_NUMBER: _ClassVar[int]
    queries_executed: _containers.RepeatedCompositeFieldContainer[QueryExecuted]
    signature: bytes
    def __init__(self, queries_executed: _Optional[_Iterable[_Union[QueryExecuted, _Mapping]]] = ..., signature: _Optional[bytes] = ...) -> None: ...

class QueryResult(_message.Message):
    __slots__ = ["bad_request", "ok", "query_id", "server_error"]
    BAD_REQUEST_FIELD_NUMBER: _ClassVar[int]
    OK_FIELD_NUMBER: _ClassVar[int]
    QUERY_ID_FIELD_NUMBER: _ClassVar[int]
    SERVER_ERROR_FIELD_NUMBER: _ClassVar[int]
    bad_request: str
    ok: OkResult
    query_id: str
    server_error: str
    def __init__(self, query_id: _Optional[str] = ..., ok: _Optional[_Union[OkResult, _Mapping]] = ..., bad_request: _Optional[str] = ..., server_error: _Optional[str] = ...) -> None: ...

class QuerySubmitted(_message.Message):
    __slots__ = ["client_id", "dataset", "query", "query_hash", "query_id", "worker_id"]
    CLIENT_ID_FIELD_NUMBER: _ClassVar[int]
    DATASET_FIELD_NUMBER: _ClassVar[int]
    QUERY_FIELD_NUMBER: _ClassVar[int]
    QUERY_HASH_FIELD_NUMBER: _ClassVar[int]
    QUERY_ID_FIELD_NUMBER: _ClassVar[int]
    WORKER_ID_FIELD_NUMBER: _ClassVar[int]
    client_id: str
    dataset: str
    query: str
    query_hash: bytes
    query_id: str
    worker_id: str
    def __init__(self, client_id: _Optional[str] = ..., worker_id: _Optional[str] = ..., query_id: _Optional[str] = ..., dataset: _Optional[str] = ..., query: _Optional[str] = ..., query_hash: _Optional[bytes] = ...) -> None: ...

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
