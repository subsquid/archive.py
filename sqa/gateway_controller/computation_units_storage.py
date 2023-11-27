import sqlite3
from .allocation import Allocation


class ComputationUnitsStorage:
    def __init__(self, data_path: str):
        self._db = sqlite3.connect(f'{data_path}/gateway.db')
        self._cursor = self._db.cursor()
        self._cursor.execute(
            'CREATE TABLE IF NOT EXISTS gateways(address varchar(42) not null primary key, workerId int, allocated int, used int, latest_block_updated int)')
        self._cursor.execute('CREATE INDEX IF NOT EXISTS gateway_index on gateways(address)')
        self._db.commit()

    def __del__(self):
        self._cursor.close()
        self._db.close()

    def increase_allocations(self, allocations: list[Allocation], worker_id: int):
        self._cursor.executemany(
            'INSERT INTO gateways VALUES(?,?,?,0,?) ON CONFLICT(address) DO UPDATE SET allocated = allocated+?, latest_block_updated=? WHERE address=? AND workerId=?',
            [(i.gateway, worker_id, i.computation_units, i.block_number, i.computation_units, i.block_number, i.gateway, worker_id)
             for i in allocations]
        )
        self._db.commit()

    def get_latest_blocks_updated(self):
        res = self._cursor.execute(f'SELECT address, latest_block_updated FROM gateways').fetchall()
        if res is None:
            return {}
        return dict(res)

    def increase_gateway_usage(self, used_units: int, gateway: str, worker_id: int):
        self._cursor.execute(
            'UPDATE SET used = used+? WHERE address=? AND workerId=?',
            (used_units, gateway, worker_id)
        )
        self._db.commit()
