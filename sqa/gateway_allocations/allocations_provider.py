import os
from typing import Tuple
import base58
from web3 import AsyncWeb3
from web3.datastructures import AttributeDict

GATEWAY_CONTRACT_CREATION_BLOCK = int(os.environ.get('GATEWAY_CONTRACT_CREATION_BLOCK', 53976941))
GATEWAY_ADDRESS = AsyncWeb3.to_checksum_address(
    os.environ.get('GATEWAY_ADDRESS', '0x9657d3dB87963d5e17dDa0746972E6958401Ab2a')
)
WORKER_REGISTRATION_ADDRESS = AsyncWeb3.to_checksum_address(
    os.environ.get('WORKER_REGISTRATION_ADDRESS', '0x6867E96A0259E68A571a368C0b8d733Aa56E3915')
)


class AllocationsProvider:
    def __init__(self, w3: AsyncWeb3):
        self._w3 = w3
        with open(f'{os.path.dirname(__file__)}/abi/GatewayRegistry.json', 'r') as abi:
            self._gateway = w3.eth.contract(address=GATEWAY_ADDRESS, abi=abi.read())
        with open(f'{os.path.dirname(__file__)}/abi/WorkerRegistration.json', 'r') as abi:
            self._worker_registration = self._w3.eth.contract(address=WORKER_REGISTRATION_ADDRESS, abi=abi.read())

    async def get_all_allocations(self, from_block=GATEWAY_CONTRACT_CREATION_BLOCK) -> Tuple[AttributeDict]:
        return await self._gateway.events.AllocatedCUs.get_logs(fromBlock=from_block)

    async def get_worker_id(self, peer_id: str) -> int:
        return await self._worker_registration.functions.workerIds(base58.b58decode(peer_id)).call()
