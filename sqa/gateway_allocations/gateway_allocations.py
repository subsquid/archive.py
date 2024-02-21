import logging
import os
import asyncio

from web3 import AsyncWeb3

from .allocations_provider import AllocationsProvider
from .computation_units_storage import ComputationUnitsStorage

LOG = logging.getLogger(__name__)
SINGLE_EXECUTION_COST = 1  # We probably don't want this to be configurable through env
POLLING_INTERVAL = int(os.environ.get('POLLING_INTERVAL', 30))


class GatewayAllocations:
    def __init__(self, rpc_url: str, peer_id: str, db_path: str):
        w3 = AsyncWeb3(AsyncWeb3.AsyncHTTPProvider(rpc_url))
        self._provider = AllocationsProvider(w3)
        self._storage = ComputationUnitsStorage(db_path)
        self._peer_id = peer_id
        self._own_id = None
        self._initialized = False

    async def _initialize_if_not(self):
        if not self._initialized:
            LOG.info("Initializing gateway allocations manager")
            await self._get_own_id()
            self._storage.initialize(self._own_id)
            self._initialized = True

    async def _get_own_id(self):
        if self._own_id is None:
            self._own_id = await self._provider.get_worker_id(self._peer_id)
            LOG.info(f'Onchain ID={self._own_id}')

    async def run(self):
        await self._initialize_if_not()
        while True:
            try:
                current_epoch = await self._provider.get_current_epoch()
                self._storage.update_epoch(current_epoch)
            except:
                LOG.exception('Error updating epoch')
            await asyncio.sleep(POLLING_INTERVAL)

    async def try_to_execute(self, gateway_id: str) -> bool:
        await self._initialize_if_not()
        LOG.debug(f"Gateway {gateway_id} trying to execute a query")
        if not self._storage.has_allocation(gateway_id):
            LOG.debug(f"No allocation for gateway {gateway_id}, checking on chain")
            cluster = await self._provider.get_gateway_cluster(gateway_id)
            allocated_cus = await self._provider.get_allocated_cus(gateway_id, self._own_id)
            if cluster is None or allocated_cus is None:
                return False
            self._storage.update_allocation(cluster, allocated_cus)
        return self._storage.try_spend_cus(gateway_id, SINGLE_EXECUTION_COST)
