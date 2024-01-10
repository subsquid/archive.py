import asyncio
import os
from typing import Tuple
import base58
from web3 import AsyncWeb3
from web3.datastructures import AttributeDict

GATEWAY_CONTRACT_CREATION_BLOCK = int(os.environ.get('GATEWAY_CONTRACT_CREATION_BLOCK', 6010034))
GATEWAY_ADDRESS = AsyncWeb3.to_checksum_address(
    os.environ.get('GATEWAY_REGISTRY_CONTRACT_ADDR', '0xC168fD9298141E3a19c624DF5692ABeeb480Fb94')
)
WORKER_REGISTRATION_ADDRESS = AsyncWeb3.to_checksum_address(
    os.environ.get('WORKER_REGISTRATION_CONTRACT_ADDR', '0x7Bf0B1ee9767eAc70A857cEbb24b83115093477F')
)
MAX_BLOCKS = int(os.environ.get('MAX_GET_LOG_BLOCKS', 1_000_000))


class AllocationsProvider:
    def __init__(self, w3: AsyncWeb3):
        self._w3 = w3
        with open(f'{os.path.dirname(__file__)}/abi/GatewayRegistry.json', 'r') as abi:
            self._gateway = w3.eth.contract(address=GATEWAY_ADDRESS, abi=abi.read())
        with open(f'{os.path.dirname(__file__)}/abi/WorkerRegistration.json', 'r') as abi:
            self._worker_registration = self._w3.eth.contract(address=WORKER_REGISTRATION_ADDRESS, abi=abi.read())

    async def get_all_allocations(self, from_block: int = None) -> (Tuple[AttributeDict], int):
        first_block = from_block if from_block is not None else GATEWAY_CONTRACT_CREATION_BLOCK
        current_block = await self._w3.eth.get_block_number()
        last_block = min(first_block + MAX_BLOCKS, current_block)
        logs = []
        while first_block < current_block:
            last_block = min(first_block + MAX_BLOCKS, current_block)
            logs += await self._gateway.events.AllocatedCUs.get_logs(fromBlock=first_block, toBlock=last_block)
            first_block = last_block + 1
            await asyncio.sleep(0.1) # So we don't get rate limited
        return logs, last_block

    async def get_worker_id(self, peer_id: str) -> int:
        return await self._worker_registration.functions.workerIds(base58.b58decode(peer_id)).call()
