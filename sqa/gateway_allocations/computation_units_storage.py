import sqlite3
from .allocation import Allocation


class ComputationUnitsStorage:
    def __init__(self, data_path: str):
        self._own_id = None
        self._db_conn = sqlite3.connect(f'{data_path}/gateway.db')

    def initialize(self, own_id: int):
        self._own_id = own_id
        with self._db_conn:
            self._db_conn.execute(
                'CREATE TABLE IF NOT EXISTS gateways(gatewayId varchar(255) not null primary key, workerId int, allocated int, used int, latest_block_updated int)')

    def __del__(self):
        self._db_conn.close()

    def _assert_initialized(self):
        assert self._own_id is not None, "GatewayStorage uninitilized"

    def increase_allocations(self, allocations: list[Allocation]):
        self._assert_initialized()

        with self._db_conn:
            self._db_conn.executemany(
                'INSERT INTO gateways VALUES(?,?,?,0,?) ON CONFLICT(gatewayId) DO UPDATE SET allocated = allocated+?, latest_block_updated=? WHERE gatewayId=? AND workerId=?',
                [(i.gateway, self._own_id, i.computation_units, i.block_number, i.computation_units, i.block_number,
                  i.gateway, self._own_id)
                 for i in allocations]
            )

    def get_latest_blocks_updated(self):
        self._assert_initialized()

        with self._db_conn:
            res = self._db_conn.execute(f'SELECT gatewayId, latest_block_updated FROM gateways WHERE workerId=?', [self._own_id]).fetchall()
            if res is None:
                return {}
            return dict(res)

    def increase_gateway_usage(self, used_units: int, gateway: str):
        self._assert_initialized()

        with self._db_conn:
            self._db_conn.execute(
                'UPDATE SET used = used+? WHERE gatewayId=? AND workerId=?',
                (used_units, gateway, self._own_id)
            )

    def get_allocations_for(self, gateways: list[str]):
        self._assert_initialized()

        gateways_list = ','.join((f'"{i}"' for i in gateways))
        with self._db_conn:
            return self._db_conn.execute(
                f'SELECT gatewayId, allocated, used FROM gateways WHERE gatewayId IN ({gateways_list}) AND workerId=?',
                [self._own_id]
            ).fetchall()
