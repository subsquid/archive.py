import logging
import os
from dataclasses import dataclass
from pathlib import Path

import base58
from web3 import AsyncWeb3

LOG = logging.getLogger(__name__)


ALLOCATIONS_VIEWER_ADDRESS = AsyncWeb3.to_checksum_address(
    os.environ.get(
        'ALLOCATIONS_VIEWER_CONTRACT_ADDR',
        '0x28d17930794d32AC1081C1A5A8284B86a6E8227E'
    )
)
GATEWAY_REGISTRY_ADDRESS = AsyncWeb3.to_checksum_address(
    os.environ.get(
        'GATEWAY_REGISTRY_CONTRACT_ADDR',
        '0xCe360FB8D0d12C508BFE7153573D3C4aB476f6A1'
    )
)
NETWORK_CONTROLLER_ADDRESS = AsyncWeb3.to_checksum_address(
    os.environ.get(
        'NETWORK_CONTROLLER_CONTRACT_ADDR',
        '0xa4285F5503D903BB10978AD652D072e79cc92F0a'
    )
)
WORKER_REGISTRATION_ADDRESS = AsyncWeb3.to_checksum_address(
    os.environ.get(
        'WORKER_REGISTRATION_CONTRACT_ADDR',
        '0x7Bf0B1ee9767eAc70A857cEbb24b83115093477F'
    )
)


def read_abi(abi_file_name: str) -> str:
    with open(Path(__file__).parent / 'abi' / abi_file_name, 'r') as abi_file:
        return abi_file.read()


ALLOCATIONS_VIEWER_ABI = read_abi('AllocationsViewer.json')
GATEWAY_REGISTRY_ABI = read_abi('GatewayRegistry.json')
NETWORK_CONTROLLER_ABI = read_abi('NetworkController.json')
STRATEGY_ABI = read_abi('Strategy.json')
WORKER_REGISTRATION_ABI = read_abi('WorkerRegistration.json')

GATEWAYS_PAGE_SIZE = 10000


@dataclass
class GatewayCluster:
    operator_addr: str
    gateway_ids: list[str]
    allocated_cus: int


class AllocationsProvider:
    def __init__(self, w3: AsyncWeb3):
        self._w3 = w3
        self._allocations_viewer = w3.eth.contract(
            ALLOCATIONS_VIEWER_ADDRESS, abi=ALLOCATIONS_VIEWER_ABI)
        self._gateway_registry = w3.eth.contract(
            GATEWAY_REGISTRY_ADDRESS, abi=GATEWAY_REGISTRY_ABI)
        self._network_controller = w3.eth.contract(
            NETWORK_CONTROLLER_ADDRESS, abi=NETWORK_CONTROLLER_ABI)
        self._worker_registration = w3.eth.contract(
            WORKER_REGISTRATION_ADDRESS, abi=WORKER_REGISTRATION_ABI)

    async def get_worker_id(self, peer_id: str) -> int:
        LOG.debug(f"Getting on-chain ID for worker {peer_id}")
        encoded_id = base58.b58decode(peer_id)
        return await self._worker_registration.functions.workerIds(encoded_id).call()

    async def get_current_epoch(self) -> int:
        LOG.debug("Getting current epoch number")
        return await self._network_controller.functions.epochNumber().call()

    async def get_all_gateways(self, worker_id: int) -> list[GatewayCluster]:
        LOG.debug("Getting all gateways")
        # We need to explicitly use block number to be sure that subsequent contract calls
        # read consistent state (list of gateways can change between blocks and mess up pagination).
        latest_block = await self._w3.eth.get_block_number()
        num_gateways = await self._gateway_registry\
            .functions\
            .getActiveGatewaysCount()\
            .call(block_identifier=latest_block)
        num_pages = (num_gateways - 1) // GATEWAYS_PAGE_SIZE + 1
        LOG.debug(f"Total {num_gateways} gateways, {num_pages} pages")

        clusters = {}
        for page in range(num_pages):
            allocations = await self._allocations_viewer\
                .functions\
                .getAllocations(worker_id, page, GATEWAYS_PAGE_SIZE)\
                .call(block_identifier=latest_block)
            for (encoded_id, allocated_cus, operator_addr) in allocations:
                gateway_id = base58.b58encode(encoded_id).decode()
                LOG.debug(f"gateway_id={gateway_id}, allocated_cus={allocated_cus}, operator={operator_addr}")
                if (cluster := clusters.get(operator_addr)) is not None:
                    cluster.gateway_ids.append(gateway_id)
                else:
                    clusters[operator_addr] = GatewayCluster(
                        operator_addr=operator_addr,
                        gateway_ids=[gateway_id],
                        allocated_cus=allocated_cus
                    )

        num_gateways_returned = sum(len(c.gateway_ids) for c in clusters.values())
        if num_gateways_returned != num_gateways:
            LOG.warning(
                f"getAllocations returned wrong number of records: "
                f"{num_gateways_returned} != {num_gateways}")

        return list(clusters.values())
