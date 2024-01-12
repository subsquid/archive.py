import logging
import sqlite3
import time
from typing import Optional

from sqa.worker.p2p.messages_pb2 import Query, QueryExecuted, InputAndOutput
from sqa.worker.p2p.util import sha3_256, size_and_hash, QueryInfo
from sqa.worker.query import QueryResult

LOG = logging.getLogger(__name__)


class LogsStorage:

    def __init__(self, local_peer_id: str, logs_db_path: str) -> None:
        self._local_peer_id = local_peer_id
        self.is_initialized = False
        self._db_conn = sqlite3.connect(logs_db_path)
        self._init_db()

    def _init_db(self):
        LOG.info("Initializing logs storage")
        with self._db_conn:
            self._db_conn.execute("CREATE TABLE IF NOT EXISTS query_logs(seq_no INTEGER PRIMARY KEY, log_msg BLOB)")
            self._db_conn.execute("CREATE TABLE IF NOT EXISTS next_seq_no(seq_no)")
            next_seq_no = self._db_conn.execute("SELECT seq_no FROM next_seq_no").fetchone()
            # If there is no next_seq_no stored in DB, wait for logs_collected message from the collector to make sure
            # we start from the right number (that's in case the worker's db was lost)
            if next_seq_no is not None:
                self.is_initialized = True
            else:
                LOG.info("Next log sequence number unknown. Waiting for message from logs collector.")

    def query_executed(self, query: Query, info: QueryInfo, result: QueryResult) -> None:
        """ Store information about successfully executed query. """
        result = InputAndOutput(
            num_read_chunks=result.num_read_chunks,
            output=size_and_hash(result.result),
        )
        query_log = self._generate_log(query, info, ok=result)
        self._store_log(query_log)

    def query_error(
            self,
            query: Query,
            info: QueryInfo,
            bad_request: Optional[str] = None,
            server_error: Optional[str] = None
    ) -> None:
        """ Store information about query execution error. """
        if bad_request is not None:
            query_log = self._generate_log(query, info, bad_request=bad_request)
        elif server_error is not None:
            query_log = self._generate_log(query, info, server_error=server_error)
        else:
            raise AssertionError("Either bad_request or server_error should be not None")
        self._store_log(query_log)

    def _generate_log(self, query: Query, info: QueryInfo, **kwargs) -> QueryExecuted:
        return QueryExecuted(
            client_id=info.client_id,
            worker_id=self._local_peer_id,
            query=query,
            query_hash=sha3_256(query.query.encode()),
            exec_time_ms=info.exec_time_ms,
            **kwargs
        )

    def _store_log(self, query_log: QueryExecuted) -> None:
        assert self.is_initialized
        LOG.debug(f"Storing query log: {query_log}")
        with self._db_conn:
            query_log.timestamp_ms = time.time_ns() // 1000_000
            self._db_conn.execute(
                "INSERT INTO query_logs SELECT seq_no, ? FROM next_seq_no",
                (query_log.SerializeToString(),)
            )
            self._db_conn.execute("UPDATE next_seq_no SET seq_no = seq_no + 1")

    def logs_collected(self, last_collected_seq_no: Optional[int]) -> None:
        """ All logs with sequence numbers up to `last_collected_seq_no` have been saved by the logs collector
            and should be discarded from the storage. """
        LOG.debug(f"Logs up to {last_collected_seq_no} collected.")
        next_seq_no = last_collected_seq_no + 1 if last_collected_seq_no is not None else 0
        with self._db_conn:
            if self.is_initialized:
                LOG.debug("Deleting collected logs")
                self._db_conn.execute("DELETE FROM query_logs WHERE seq_no < ?", (next_seq_no,))
            else:
                LOG.info("Initializing logs storage")
                self._db_conn.execute("INSERT INTO next_seq_no VALUES(?)", (next_seq_no,))
                self.is_initialized = True

    def get_logs(self) -> list[QueryExecuted]:
        """ Get all stored query logs. """
        def log_from_db(seq_no, log_msg):
            log = QueryExecuted.FromString(log_msg)
            log.seq_no = seq_no
            return log
        with self._db_conn:
            rows = self._db_conn.execute("SELECT seq_no, log_msg FROM query_logs").fetchall()
            return [log_from_db(*r) for r in rows]


