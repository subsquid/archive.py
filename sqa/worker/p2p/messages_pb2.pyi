from google.protobuf import empty_pb2 as _empty_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class Envelope(_message.Message):
    __slots__ = ("pong", "ping", "query", "query_result", "query_submitted", "query_finished", "query_logs", "logs_collected")
    PONG_FIELD_NUMBER: _ClassVar[int]
    PING_FIELD_NUMBER: _ClassVar[int]
    QUERY_FIELD_NUMBER: _ClassVar[int]
    QUERY_RESULT_FIELD_NUMBER: _ClassVar[int]
    QUERY_SUBMITTED_FIELD_NUMBER: _ClassVar[int]
    QUERY_FINISHED_FIELD_NUMBER: _ClassVar[int]
    QUERY_LOGS_FIELD_NUMBER: _ClassVar[int]
    LOGS_COLLECTED_FIELD_NUMBER: _ClassVar[int]
    pong: Pong
    ping: Ping
    query: Query
    query_result: QueryResult
    query_submitted: QuerySubmitted
    query_finished: QueryFinished
    query_logs: QueryLogs
    logs_collected: LogsCollected
    def __init__(self, pong: _Optional[_Union[Pong, _Mapping]] = ..., ping: _Optional[_Union[Ping, _Mapping]] = ..., query: _Optional[_Union[Query, _Mapping]] = ..., query_result: _Optional[_Union[QueryResult, _Mapping]] = ..., query_submitted: _Optional[_Union[QuerySubmitted, _Mapping]] = ..., query_finished: _Optional[_Union[QueryFinished, _Mapping]] = ..., query_logs: _Optional[_Union[QueryLogs, _Mapping]] = ..., logs_collected: _Optional[_Union[LogsCollected, _Mapping]] = ...) -> None: ...

class Range(_message.Message):
    __slots__ = ("begin", "end")
    BEGIN_FIELD_NUMBER: _ClassVar[int]
    END_FIELD_NUMBER: _ClassVar[int]
    begin: int
    end: int
    def __init__(self, begin: _Optional[int] = ..., end: _Optional[int] = ...) -> None: ...

class RangeSet(_message.Message):
    __slots__ = ("ranges",)
    RANGES_FIELD_NUMBER: _ClassVar[int]
    ranges: _containers.RepeatedCompositeFieldContainer[Range]
    def __init__(self, ranges: _Optional[_Iterable[_Union[Range, _Mapping]]] = ...) -> None: ...

class WorkerState(_message.Message):
    __slots__ = ("datasets",)
    class DatasetsEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: RangeSet
        def __init__(self, key: _Optional[str] = ..., value: _Optional[_Union[RangeSet, _Mapping]] = ...) -> None: ...
    DATASETS_FIELD_NUMBER: _ClassVar[int]
    datasets: _containers.MessageMap[str, RangeSet]
    def __init__(self, datasets: _Optional[_Mapping[str, RangeSet]] = ...) -> None: ...

class DatasetRanges(_message.Message):
    __slots__ = ("url", "ranges")
    URL_FIELD_NUMBER: _ClassVar[int]
    RANGES_FIELD_NUMBER: _ClassVar[int]
    url: str
    ranges: _containers.RepeatedCompositeFieldContainer[Range]
    def __init__(self, url: _Optional[str] = ..., ranges: _Optional[_Iterable[_Union[Range, _Mapping]]] = ...) -> None: ...

class Ping(_message.Message):
    __slots__ = ("worker_id", "version", "stored_bytes", "stored_ranges", "signature")
    WORKER_ID_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    STORED_BYTES_FIELD_NUMBER: _ClassVar[int]
    STORED_RANGES_FIELD_NUMBER: _ClassVar[int]
    SIGNATURE_FIELD_NUMBER: _ClassVar[int]
    worker_id: str
    version: str
    stored_bytes: int
    stored_ranges: _containers.RepeatedCompositeFieldContainer[DatasetRanges]
    signature: bytes
    def __init__(self, worker_id: _Optional[str] = ..., version: _Optional[str] = ..., stored_bytes: _Optional[int] = ..., stored_ranges: _Optional[_Iterable[_Union[DatasetRanges, _Mapping]]] = ..., signature: _Optional[bytes] = ...) -> None: ...

class Pong(_message.Message):
    __slots__ = ("ping_hash", "not_registered", "unsupported_version", "jailed_v1", "active", "jailed")
    PING_HASH_FIELD_NUMBER: _ClassVar[int]
    NOT_REGISTERED_FIELD_NUMBER: _ClassVar[int]
    UNSUPPORTED_VERSION_FIELD_NUMBER: _ClassVar[int]
    JAILED_V1_FIELD_NUMBER: _ClassVar[int]
    ACTIVE_FIELD_NUMBER: _ClassVar[int]
    JAILED_FIELD_NUMBER: _ClassVar[int]
    ping_hash: bytes
    not_registered: _empty_pb2.Empty
    unsupported_version: _empty_pb2.Empty
    jailed_v1: _empty_pb2.Empty
    active: WorkerState
    jailed: str
    def __init__(self, ping_hash: _Optional[bytes] = ..., not_registered: _Optional[_Union[_empty_pb2.Empty, _Mapping]] = ..., unsupported_version: _Optional[_Union[_empty_pb2.Empty, _Mapping]] = ..., jailed_v1: _Optional[_Union[_empty_pb2.Empty, _Mapping]] = ..., active: _Optional[_Union[WorkerState, _Mapping]] = ..., jailed: _Optional[str] = ...) -> None: ...

class Query(_message.Message):
    __slots__ = ("query_id", "dataset", "query", "profiling", "client_state_json", "signature")
    QUERY_ID_FIELD_NUMBER: _ClassVar[int]
    DATASET_FIELD_NUMBER: _ClassVar[int]
    QUERY_FIELD_NUMBER: _ClassVar[int]
    PROFILING_FIELD_NUMBER: _ClassVar[int]
    CLIENT_STATE_JSON_FIELD_NUMBER: _ClassVar[int]
    SIGNATURE_FIELD_NUMBER: _ClassVar[int]
    query_id: str
    dataset: str
    query: str
    profiling: bool
    client_state_json: str
    signature: bytes
    def __init__(self, query_id: _Optional[str] = ..., dataset: _Optional[str] = ..., query: _Optional[str] = ..., profiling: bool = ..., client_state_json: _Optional[str] = ..., signature: _Optional[bytes] = ...) -> None: ...

class QueryResult(_message.Message):
    __slots__ = ("query_id", "ok", "bad_request", "server_error", "no_allocation")
    QUERY_ID_FIELD_NUMBER: _ClassVar[int]
    OK_FIELD_NUMBER: _ClassVar[int]
    BAD_REQUEST_FIELD_NUMBER: _ClassVar[int]
    SERVER_ERROR_FIELD_NUMBER: _ClassVar[int]
    NO_ALLOCATION_FIELD_NUMBER: _ClassVar[int]
    query_id: str
    ok: OkResult
    bad_request: str
    server_error: str
    no_allocation: _empty_pb2.Empty
    def __init__(self, query_id: _Optional[str] = ..., ok: _Optional[_Union[OkResult, _Mapping]] = ..., bad_request: _Optional[str] = ..., server_error: _Optional[str] = ..., no_allocation: _Optional[_Union[_empty_pb2.Empty, _Mapping]] = ...) -> None: ...

class OkResult(_message.Message):
    __slots__ = ("data", "exec_plan")
    DATA_FIELD_NUMBER: _ClassVar[int]
    EXEC_PLAN_FIELD_NUMBER: _ClassVar[int]
    data: bytes
    exec_plan: bytes
    def __init__(self, data: _Optional[bytes] = ..., exec_plan: _Optional[bytes] = ...) -> None: ...

class QuerySubmitted(_message.Message):
    __slots__ = ("client_id", "worker_id", "query_id", "dataset", "query", "query_hash")
    CLIENT_ID_FIELD_NUMBER: _ClassVar[int]
    WORKER_ID_FIELD_NUMBER: _ClassVar[int]
    QUERY_ID_FIELD_NUMBER: _ClassVar[int]
    DATASET_FIELD_NUMBER: _ClassVar[int]
    QUERY_FIELD_NUMBER: _ClassVar[int]
    QUERY_HASH_FIELD_NUMBER: _ClassVar[int]
    client_id: str
    worker_id: str
    query_id: str
    dataset: str
    query: str
    query_hash: bytes
    def __init__(self, client_id: _Optional[str] = ..., worker_id: _Optional[str] = ..., query_id: _Optional[str] = ..., dataset: _Optional[str] = ..., query: _Optional[str] = ..., query_hash: _Optional[bytes] = ...) -> None: ...

class QueryFinished(_message.Message):
    __slots__ = ("client_id", "worker_id", "query_id", "exec_time_ms", "ok", "bad_request", "server_error", "timeout", "no_allocation")
    CLIENT_ID_FIELD_NUMBER: _ClassVar[int]
    WORKER_ID_FIELD_NUMBER: _ClassVar[int]
    QUERY_ID_FIELD_NUMBER: _ClassVar[int]
    EXEC_TIME_MS_FIELD_NUMBER: _ClassVar[int]
    OK_FIELD_NUMBER: _ClassVar[int]
    BAD_REQUEST_FIELD_NUMBER: _ClassVar[int]
    SERVER_ERROR_FIELD_NUMBER: _ClassVar[int]
    TIMEOUT_FIELD_NUMBER: _ClassVar[int]
    NO_ALLOCATION_FIELD_NUMBER: _ClassVar[int]
    client_id: str
    worker_id: str
    query_id: str
    exec_time_ms: int
    ok: SizeAndHash
    bad_request: str
    server_error: str
    timeout: _empty_pb2.Empty
    no_allocation: _empty_pb2.Empty
    def __init__(self, client_id: _Optional[str] = ..., worker_id: _Optional[str] = ..., query_id: _Optional[str] = ..., exec_time_ms: _Optional[int] = ..., ok: _Optional[_Union[SizeAndHash, _Mapping]] = ..., bad_request: _Optional[str] = ..., server_error: _Optional[str] = ..., timeout: _Optional[_Union[_empty_pb2.Empty, _Mapping]] = ..., no_allocation: _Optional[_Union[_empty_pb2.Empty, _Mapping]] = ...) -> None: ...

class QueryExecuted(_message.Message):
    __slots__ = ("client_id", "worker_id", "query", "query_hash", "exec_time_ms", "ok", "bad_request", "server_error", "seq_no", "timestamp_ms", "signature")
    CLIENT_ID_FIELD_NUMBER: _ClassVar[int]
    WORKER_ID_FIELD_NUMBER: _ClassVar[int]
    QUERY_FIELD_NUMBER: _ClassVar[int]
    QUERY_HASH_FIELD_NUMBER: _ClassVar[int]
    EXEC_TIME_MS_FIELD_NUMBER: _ClassVar[int]
    OK_FIELD_NUMBER: _ClassVar[int]
    BAD_REQUEST_FIELD_NUMBER: _ClassVar[int]
    SERVER_ERROR_FIELD_NUMBER: _ClassVar[int]
    SEQ_NO_FIELD_NUMBER: _ClassVar[int]
    TIMESTAMP_MS_FIELD_NUMBER: _ClassVar[int]
    SIGNATURE_FIELD_NUMBER: _ClassVar[int]
    client_id: str
    worker_id: str
    query: Query
    query_hash: bytes
    exec_time_ms: int
    ok: InputAndOutput
    bad_request: str
    server_error: str
    seq_no: int
    timestamp_ms: int
    signature: bytes
    def __init__(self, client_id: _Optional[str] = ..., worker_id: _Optional[str] = ..., query: _Optional[_Union[Query, _Mapping]] = ..., query_hash: _Optional[bytes] = ..., exec_time_ms: _Optional[int] = ..., ok: _Optional[_Union[InputAndOutput, _Mapping]] = ..., bad_request: _Optional[str] = ..., server_error: _Optional[str] = ..., seq_no: _Optional[int] = ..., timestamp_ms: _Optional[int] = ..., signature: _Optional[bytes] = ...) -> None: ...

class QueryLogs(_message.Message):
    __slots__ = ("queries_executed",)
    QUERIES_EXECUTED_FIELD_NUMBER: _ClassVar[int]
    queries_executed: _containers.RepeatedCompositeFieldContainer[QueryExecuted]
    def __init__(self, queries_executed: _Optional[_Iterable[_Union[QueryExecuted, _Mapping]]] = ...) -> None: ...

class InputAndOutput(_message.Message):
    __slots__ = ("num_read_chunks", "output")
    NUM_READ_CHUNKS_FIELD_NUMBER: _ClassVar[int]
    OUTPUT_FIELD_NUMBER: _ClassVar[int]
    num_read_chunks: int
    output: SizeAndHash
    def __init__(self, num_read_chunks: _Optional[int] = ..., output: _Optional[_Union[SizeAndHash, _Mapping]] = ...) -> None: ...

class SizeAndHash(_message.Message):
    __slots__ = ("size", "sha3_256")
    SIZE_FIELD_NUMBER: _ClassVar[int]
    SHA3_256_FIELD_NUMBER: _ClassVar[int]
    size: int
    sha3_256: bytes
    def __init__(self, size: _Optional[int] = ..., sha3_256: _Optional[bytes] = ...) -> None: ...

class LogsCollected(_message.Message):
    __slots__ = ("sequence_numbers",)
    class SequenceNumbersEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: int
        def __init__(self, key: _Optional[str] = ..., value: _Optional[int] = ...) -> None: ...
    SEQUENCE_NUMBERS_FIELD_NUMBER: _ClassVar[int]
    sequence_numbers: _containers.ScalarMap[str, int]
    def __init__(self, sequence_numbers: _Optional[_Mapping[str, int]] = ...) -> None: ...
