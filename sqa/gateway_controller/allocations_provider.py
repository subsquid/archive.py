import os
from typing import Tuple
import base58
from web3 import Web3
from web3.datastructures import AttributeDict

GATEWAY_CONTRACT_CREATION_BLOCK = 53976941


class AllocationsProvider:
    def __init__(self, w3: Web3):
        self._w3 = w3
        gateway_address = Web3.to_checksum_address('0x396ba795fbcc4033caad8472b46eb76723377e6e')

        with open(f'{os.path.dirname(__file__)}/abi/GatewayRegistry.json', 'r') as abi:
            self.gateway = w3.eth.contract(address=gateway_address, abi=abi.read())

    def get_all_allocations(self, from_block=GATEWAY_CONTRACT_CREATION_BLOCK) -> Tuple[AttributeDict]:
        return self.gateway.events.AllocatedCUs.get_logs(fromBlock=from_block)

    def get_worker_id(self, peer_id: str):
        worker_registration_address = Web3.to_checksum_address('0x6867E96A0259E68A571a368C0b8d733Aa56E3915')
        with open(f'{os.path.dirname(__file__)}/abi/WorkerRegistration.json', 'r') as abi:
            worker_registration = self._w3.eth.contract(address=worker_registration_address, abi=abi.read())
            return worker_registration.functions.workerIds(base58.b58decode(peer_id)).call()
