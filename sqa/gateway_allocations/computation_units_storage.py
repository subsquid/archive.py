import logging
import sqlite3
from typing import Optional

from sqa.gateway_allocations.allocations_provider import GatewayCluster

LOG = logging.getLogger(__name__)

# Bump when changing DB schema
SCHEMA_VERSION = 1

GET_TABLES = "SELECT name FROM sqlite_master WHERE type = 'table'"

INIT_DB = f"""
PRAGMA user_version = {SCHEMA_VERSION};

CREATE TABLE IF NOT EXISTS operators(
    address STRING PRIMARY KEY,
    allocated_cus INTEGER NOT NULL DEFAULT 0,
    spent_cus INTEGER NOT NULL DEFAULT 0,
    epoch INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS gateways(
    gateway_id STRING PRIMARY KEY,
    operator_addr STRING NOT NULL,
    FOREIGN KEY (operator_addr) 
        REFERENCES operators(address)
        ON DELETE CASCADE
);
"""

CHECK_ALLOCATION = "SELECT COUNT(*) FROM gateways WHERE gateway_id = ?1"

SPEND_CUS = """
UPDATE operators
SET spent_cus = spent_cus + ?2
WHERE allocated_cus - spent_cus >= ?2
AND address IN (SELECT operator_addr FROM gateways WHERE gateway_id = ?1)
"""

CLEAN_OLD_ALLOCATIONS = "DELETE FROM operators WHERE epoch < ?1"

UPDATE_ALLOCATION = """
INSERT INTO operators (address, allocated_cus, spent_cus, epoch)
VALUES (?1, ?2, 0, ?3)
ON CONFLICT DO UPDATE SET allocated_cus=excluded.allocated_cus, epoch=excluded.epoch
"""

ADD_GATEWAY = "INSERT OR REPLACE INTO gateways (gateway_id, operator_addr) VALUES (?1, ?2)"

GET_EPOCH = "SELECT COALESCE(MAX(epoch), 0) FROM operators"


class ComputationUnitsStorage:
    def __init__(self, db_path: str):
        self._own_id: 'Optional[int]' = None
        self._epoch: 'Optional[int]' = None
        self._db_conn = sqlite3.connect(db_path)

    def initialize(self, own_id: int):
        with self._db_conn:
            self._db_conn.execute("PRAGMA foreign_keys = ON")
            try:
                schema_version = self._db_conn.execute("PRAGMA user_version").fetchone()[0]
            except (sqlite3.Error, KeyError):
                schema_version = 0
            if schema_version < SCHEMA_VERSION:
                LOG.info(f"Upgrading allocations DB schema from version {schema_version} to {SCHEMA_VERSION}")
                for table in self._db_conn.execute(GET_TABLES).fetchall():
                    self._db_conn.execute(f"DROP TABLE {table[0]}")
                self._db_conn.execute("VACUUM")
                self._db_conn.executescript(INIT_DB)
                LOG.info("Allocations DB upgraded")

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

    def update_allocation(self, cluster: GatewayCluster, allocated_cus: int) -> None:
        self._assert_initialized()
        LOG.debug(f"Operator {cluster.operator_addr} allocated {allocated_cus}")
        with self._db_conn:
            self._db_conn.execute(
                UPDATE_ALLOCATION,
                (cluster.operator_addr, allocated_cus, self._epoch)
            )
            for gateway_id in cluster.gateway_ids:
                self._db_conn.execute(ADD_GATEWAY, (gateway_id, cluster.operator_addr))
