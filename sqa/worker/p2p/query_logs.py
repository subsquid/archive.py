import logging
from typing import Optional

from sqa.worker.p2p.messages_pb2 import Query, QueryLogs, QueryExecuted, InputAndOutput
from sqa.worker.p2p.util import sha3_256, size_and_hash, QueryInfo
from sqa.worker.query import QueryResult

LOG = logging.getLogger(__name__)


class LogsStorage:

    def __init__(self, local_peer_id: str) -> None:
        self._local_peer_id = local_peer_id
        self._next_seq_no = 0
        self._logs: [QueryExecuted] = []

    def query_executed(self, query: Query, info: QueryInfo, result: QueryResult) -> None:
        """ Store information about successfully executed query. """
        query_log = self._generate_log(query, info)
        query_log.ok = InputAndOutput(
            num_read_chunks=result.num_read_chunks,
            output=size_and_hash(result.gzipped_bytes),
        )
        self._store_log(query_log)

    def query_error(
            self,
            query: Query,
            info: QueryInfo,
            bad_request: Optional[str] = None,
            server_error: Optional[str] = None
    ) -> None:
        """ Store information about query execution error. """
        query_log = self._generate_log(query, info)
        if bad_request is not None:
            query_log.bad_request = bad_request
        elif server_error is not None:
            query_log.server_error = server_error
        self._store_log(query_log)

    def _generate_log(self, query: Query, info: QueryInfo) -> QueryExecuted:
        return QueryExecuted(
            client_id=info.client_id,
            worker_id=self._local_peer_id,
            query=query,
            query_hash=sha3_256(query.query.encode()),
            exec_time_ms=info.exec_time_ms,
        )

    def _store_log(self, query_log: QueryExecuted) -> None:
        query_log.seq_no = self._next_seq_no
        LOG.debug(f"Storing query log: {query_log}")
        self._logs.append(query_log)
        self._next_seq_no += 1

    def logs_collected(self, last_collected_seq_no: int) -> None:
        """ All logs with sequence numbers up to `last_collected_seq_no` have been saved by the logs collector
            and should be discarded from the storage. """
        LOG.debug(f"Logs up to {last_collected_seq_no} collected.")
        next_seq_no = last_collected_seq_no + 1
        self._logs = [log for log in self._logs if log.seq_no >= next_seq_no]
        if self._next_seq_no < next_seq_no:
            # There are some logs saved by the collector that we miss (possible when worker restarts).
            # We need to re-number the stored logs so that they're not rejected by the collector as duplicates.
            LOG.debug(f"Updating log sequence number {self._next_seq_no} -> {next_seq_no}")
            for seq_no, log in enumerate(self._logs, start=next_seq_no):
                log.seq_no = seq_no
            self._next_seq_no = next_seq_no

    def get_logs(self) -> QueryLogs:
        """ Get all stored query logs. """
        return QueryLogs(queries_executed=self._logs.copy())


