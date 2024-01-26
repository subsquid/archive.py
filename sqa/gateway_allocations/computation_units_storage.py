import logging
import sqlite3
from typing import Optional

LOG = logging.getLogger(__name__)

CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS gateway_allocations(
    gateway_id STRING PRIMARY KEY,
    allocated_cus INTEGER NOT NULL DEFAULT 0,
    spent_cus INTEGER NOT NULL DEFAULT 0,
    epoch INTEGER NOT NULL
)"""

CHECK_ALLOCATION = "SELECT COUNT(*) FROM gateway_allocations WHERE gateway_id = ?1"

SPEND_CUS = """
UPDATE gateway_allocations
SET spent_cus = spent_cus + ?2
WHERE gateway_id = ?1 AND allocated_cus - spent_cus >= ?2
"""

CLEAN_OLD_ALLOCATIONS = "DELETE FROM gateway_allocations WHERE epoch < ?1"

UPDATE_ALLOCATION = """
INSERT INTO gateway_allocations (gateway_id, allocated_cus, spent_cus, epoch)
VALUES (?1, ?2, 0, ?3)
"""

GET_EPOCH = "SELECT COALESCE(MAX(epoch), 0) FROM gateway_allocations"


class ComputationUnitsStorage:
    def __init__(self, db_path: str):
        self._own_id: 'Optional[int]' = None
        self._epoch: 'Optional[int]' = None
        self._db_conn = sqlite3.connect(db_path)

    def initialize(self, own_id: int):
        with self._db_conn:
            self._db_conn.execute(CREATE_TABLE)
            self._epoch = self._db_conn.execute(GET_EPOCH).fetchone()[0]
        self._own_id = own_id

    def __del__(self):
        self._db_conn.close()

    def _assert_initialized(self):
        assert self._own_id is not None, "GatewayStorage uninitialized"

    def update_epoch(self, current_epoch: int) -> None:
        self._assert_initialized()
        if current_epoch > self._epoch:
            LOG.info(f"New epoch started: {current_epoch}")
            with self._db_conn:
                self._db_conn.execute(CLEAN_OLD_ALLOCATIONS, (current_epoch,))
            self._epoch = current_epoch

    def has_allocation(self, gateway_id: str) -> bool:
        self._assert_initialized()
        LOG.debug(f"Checking if gateway {gateway_id} has allocation")
        with self._db_conn:
            return self._db_conn.execute(CHECK_ALLOCATION, (gateway_id,)).fetchone()[0]

    def try_spend_cus(self, gateway_id: str, used_units: int) -> bool:
        self._assert_initialized()
        LOG.debug(f"Gateway {gateway_id} spending {used_units} CUs")
        with self._db_conn:
            return self._db_conn.execute(SPEND_CUS, (gateway_id, used_units)).rowcount > 0

    def update_allocation(self, gateway_id: str, allocated_cus: int) -> None:
        self._assert_initialized()
        LOG.debug(f"Gateway {gateway_id} allocated {allocated_cus}")
        with self._db_conn:
            self._db_conn.execute(
                UPDATE_ALLOCATION,
                (gateway_id, allocated_cus, self._epoch)
            )
