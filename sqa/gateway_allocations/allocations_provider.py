import logging
import os
from pathlib import Path

import base58
from web3 import AsyncWeb3

LOG = logging.getLogger(__name__)

GATEWAY_REGISTRY_ADDRESS = AsyncWeb3.to_checksum_address(
    os.environ.get(
        'GATEWAY_REGISTRY_CONTRACT_ADDR',
        '0x70FCB652F81436d8f1AFDc9F15085Fb2274281E8'
    )
)
WORKER_REGISTRATION_ADDRESS = AsyncWeb3.to_checksum_address(
    os.environ.get(
        'WORKER_REGISTRATION_CONTRACT_ADDR',
        '0x7Bf0B1ee9767eAc70A857cEbb24b83115093477F'
    )
)
NETWORK_CONTROLLER_ADDRESS = AsyncWeb3.to_checksum_address(
    os.environ.get(
        'NETWORK_CONTROLLER_CONTRACT_ADDR',
        '0xa4285F5503D903BB10978AD652D072e79cc92F0a'
    )
)


def read_abi(abi_file_name: str) -> str:
    with open(Path(__file__).parent / 'abi' / abi_file_name, 'r') as abi_file:
        return abi_file.read()


GATEWAY_REGISTRY_ABI = read_abi('GatewayRegistry.json')
WORKER_REGISTRATION_ABI = read_abi('WorkerRegistration.json')
NETWORK_CONTROLLER_ABI = read_abi('NetworkController.json')
STRATEGY_ABI = read_abi('Strategy.json')


class AllocationsProvider:
    def __init__(self, w3: AsyncWeb3):
        self._w3 = w3
        self._gateway_registry = w3.eth.contract(
            GATEWAY_REGISTRY_ADDRESS, abi=GATEWAY_REGISTRY_ABI)
        self._worker_registration = w3.eth.contract(
            WORKER_REGISTRATION_ADDRESS, abi=WORKER_REGISTRATION_ABI)
        self._network_controller = w3.eth.contract(
            NETWORK_CONTROLLER_ADDRESS, abi=NETWORK_CONTROLLER_ABI)

    async def get_worker_id(self, peer_id: str) -> int:
        LOG.debug(f"Getting on-chain ID for worker {peer_id}")
        encoded_id = base58.b58decode(peer_id)
        return await self._worker_registration.functions.workerIds(encoded_id).call()

    async def get_current_epoch(self) -> int:
        LOG.debug("Getting current epoch number")
        return await self._network_controller.functions.epochNumber().call()

    async def get_allocated_cus(self, gateway_id: str, worker_id: int) -> int:
        LOG.debug(f"Getting allocated CUs for gateway {gateway_id}")
        encoded_id = base58.b58decode(gateway_id)
        strategy_addr = await self._gateway_registry.functions.getUsedStrategy(encoded_id).call()
        checksum_addr = AsyncWeb3.to_checksum_address(strategy_addr)
        strategy = self._w3.eth.contract(checksum_addr, abi=STRATEGY_ABI)
        return await strategy.functions.computationUnitsPerEpoch(encoded_id, worker_id).call()
