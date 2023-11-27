import logging

from web3 import Web3
from web3.datastructures import AttributeDict

from .allocations_provider import AllocationsProvider
from .computation_units_storage import ComputationUnitsStorage
from .allocation import Allocation

LOG = logging.getLogger(__name__)


class Gateways:
    def __init__(self, peer_id: str, data_path: str):
        w3 = Web3(Web3.HTTPProvider('https://arbitrum-goerli.infura.io/v3/e4b66244e61a4149af62215a6d907226'))
        self._provider = AllocationsProvider(w3)
        self._storage = ComputationUnitsStorage(data_path)
        self._own_id = self._provider.get_worker_id(peer_id)

    def update(self):
        allocations = self._filter_out_old_allocations(
            self._filter_own_allocations(self._provider.get_all_allocations())
        )
        LOG.info(self._storage._cursor.execute(
            'SELECT * FROM gateways WHERE workerId=?', [self._own_id]
        ).fetchall())

        self._storage.increase_allocations(allocations, self._own_id)

    def on_query_executed(self, gateway_id: str):
        self._storage.increase_gateway_usage(1, gateway_id, self._own_id)

    def _filter_own_allocations(self, allocations: tuple[AttributeDict]):
        return [
            Allocation(
                alloc['args']['gateway'],
                alloc['args']['cus'][(alloc['args']['workerIds']).index(self._own_id)],
                alloc['blockNumber']
            )
            for alloc in allocations if self._own_id in alloc['args']['workerIds']
        ]

    def _filter_out_old_allocations(self, allocations: list[Allocation]):
        blocks_updates = self._storage.get_latest_blocks_updated()
        return [
            alloc for alloc in allocations if
            alloc.gateway not in blocks_updates or alloc.block_number > blocks_updates[alloc.gateway]
        ]
