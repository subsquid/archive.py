import asyncio
import logging
import os
import time

from web3 import AsyncWeb3

from .allocations_provider import AllocationsProvider
from .computation_units_storage import ComputationUnitsStorage

LOG = logging.getLogger(__name__)
SINGLE_EXECUTION_COST = 1  # We probably don't want this to be configurable through env
POLLING_INTERVAL = int(os.environ.get('POLLING_INTERVAL', 30))


class GatewayAllocations:
    @classmethod
    async def new(cls, rpc_url: str, peer_id: str, db_path: str):
        self = cls(rpc_url, peer_id, db_path)
        LOG.info("Initializing gateway allocations manager")
        self._own_id = await self._provider.get_worker_id(self._peer_id)
        LOG.info(f'Onchain ID={self._own_id}')
        self._storage.initialize(self._own_id)
        return self

    def __init__(self, rpc_url: str, peer_id: str, db_path: str):
        w3 = AsyncWeb3(AsyncWeb3.AsyncHTTPProvider(rpc_url))
        self._provider = AllocationsProvider(w3)
        self._storage = ComputationUnitsStorage(db_path)
        self._peer_id = peer_id
        self._own_id = None
        self._lock = asyncio.Lock()

    async def run(self):
        while True:
            try:
                current_epoch = await self._provider.get_current_epoch()
                if self._storage.update_epoch(current_epoch):
                    LOG.info("New epoch started. Updating allocations")
                    start = time.time()
                    clusters = await self._provider.get_all_gateways(self._own_id)
                    self._storage.update_allocations(clusters)
                    end = time.time()
                    LOG.info(f"Allocations updated in {int((end - start) * 1000)} ms")
            except:
                LOG.exception('Error updating epoch')
            await asyncio.sleep(POLLING_INTERVAL)

    async def try_to_execute(self, gateway_id: str) -> bool:
        LOG.debug(f"Gateway {gateway_id} trying to execute a query")
        async with self._lock:
            return self._storage.try_spend_cus(gateway_id, SINGLE_EXECUTION_COST)
