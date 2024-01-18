import logging
import os
import asyncio

import base58
from web3 import AsyncWeb3
from web3.datastructures import AttributeDict

from .allocations_provider import AllocationsProvider
from .computation_units_storage import ComputationUnitsStorage
from .allocation import Allocation

LOG = logging.getLogger(__name__)
SINGLE_EXECUTION_COST = 1  # We probably don't want this to be configurable through env
EVENT_POLLING_INTERVAL = int(os.environ.get('POLLING_INTERVAL', 30))
ALLOCATIONS_ENABLED = bool(os.environ.get('ENABLE_ALLOCATIONS', False))


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
            await self._get_own_id()
            self._storage.initialize(self._own_id)
            self._initialized = True

    async def run(self):
        await self._initialize_if_not()
        while True:
            try:
                await self._update()
            except:
                LOG.exception('Error in RPC')
            await asyncio.sleep(EVENT_POLLING_INTERVAL)

    async def try_to_execute(self, gateway_peer_id: str) -> bool:
        if not ALLOCATIONS_ENABLED:
            return True
        await self._initialize_if_not()
        gateway_id = self._b58_id_to_hex(gateway_peer_id)
        if not self._can_execute(gateway_id):
            return False
        self._storage.increase_gateway_usage(SINGLE_EXECUTION_COST, gateway_id)
        return True

    def _can_execute(self, gateway_id: str) -> bool:
        allocations = self._storage.get_allocations_for([gateway_id])
        if not allocations:
            return False
        (_, allocated, used) = allocations[0]
        return allocated >= used + SINGLE_EXECUTION_COST

    async def _update(self):
        block_number = self._storage.latest_update_block()
        (logs, last_scanned_block) = await self._provider.get_all_allocations(block_number)
        allocations = self._filter_out_old_allocations(
            self._filter_own_allocations(logs)
        )
        if len(allocations) == 0:
            self._storage.update_latest_block_scanned(last_scanned_block)
            return
        self._storage.increase_allocations(allocations)
        self._storage.update_latest_block_scanned(last_scanned_block)
        gateways_updated = list(set([i.gateway for i in allocations]))
        updated_gateway_allocations = self._storage.get_allocations_for(gateways_updated)
        summary = '; '.join([f'Client {gateway}, Total: {allocated}, Used: {used}' for (gateway, allocated, used) in
                             updated_gateway_allocations])
        LOG.info(f'Allocations update: {summary}')

    async def _get_own_id(self):
        if self._own_id is None:
            self._own_id = await self._provider.get_worker_id(self._peer_id)
            LOG.info(f'Onchain ID={self._own_id}')

    def _filter_own_allocations(self, allocations: tuple[AttributeDict]):
        return [
            Allocation(
                alloc['args']['peerId'].hex(),
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

    @staticmethod
    def _b58_id_to_hex(peer_id: str) -> str:
        return base58.b58decode(peer_id).hex()