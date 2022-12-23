from typing import Optional, NamedTuple, AsyncIterator
import asyncio

from .rpc import RpcClient, RpcCall
from .model import Block, Log


class IngestOptions(NamedTuple):
    rpc: RpcClient
    from_block: Optional[int]
    to_block: Optional[int]
    concurrency: Optional[int]


class Ingest:
    @staticmethod
    def get_blocks(options: IngestOptions) -> AsyncIterator[list[Block]]:
        return Ingest(options).loop()

    def __init__(self, options: IngestOptions):
        self._rpc = options.rpc
        self._height = (options.from_block or 0) - 1
        self._end = options.to_block
        self._concurrency = options.concurrency or 5
        self._chain_height = 0
        self._strides = []
        self._stride_size = 10

    async def loop(self) -> AsyncIterator[list[Block]]:
        while not self._is_finished() or len(self._strides):
            try:
                blocks = await self._strides.pop(0)
            except IndexError:
                await self._wait_chain()
                self._schedule_strides()
            else:
                self._schedule_strides()
                yield blocks

    async def _wait_chain(self):
        stride_size = self._stride_size
        if self._end is not None:
            stride_size = min(stride_size, self._end - self._height)
        if self._dist() < stride_size:
            self._chain_height = await self._get_chain_height()

        while self._dist() <= 0:
            await asyncio.sleep(2)
            self._chain_height = await self._get_chain_height()

    async def _get_chain_height(self) -> int:
        hex = await self._rpc.call('eth_blockNumber')
        height = int(hex, 0)
        return max(height - 10, 0)

    async def _fetch_stride(self, from_block: int, to_block: int) -> list[Block]:
        calls = []
        for i in range(from_block, to_block + 1):
            calls.append(RpcCall('eth_getBlockByNumber', [hex(i), True]))
        calls.append(RpcCall('eth_getLogs', [{
            'fromBlock': hex(from_block),
            'toBlock': hex(to_block),
        }]))

        response = await self._rpc.batch(calls)

        blocks: list[Block] = []
        for i in range(len(response) - 1):
            raw = response[i]
            transactions = []
            for tx in raw['transactions']:
                transactions.append({
                    'blockNumber': int(tx['blockNumber'], 0),
                    'transactionIndex': int(tx['transactionIndex'], 0),
                    'hash': tx['hash'],
                    'gas': int(tx['gas'], 0),
                    'gasPrice': int(tx['gasPrice'], 0),
                    'from': tx['from'],
                    'to': tx['to'],
                    'sighash': tx['input'][:10] if len(tx['input']) > 10 else None,
                    'input': tx['input'],
                    'nonce': int(tx['nonce'], 0),
                    'value': int(tx['value'], 0),
                    'v': str(int(tx['v'], 0)),
                    's': str(int(tx['s'], 0)),
                    'r': str(int(tx['r'], 0))
                })
            blocks.append({
                'header': {
                    'number': int(raw['number'], 0),
                    'hash': raw['hash'],
                    'parentHash': raw['parentHash'],
                    'nonce': raw['nonce'],
                    'sha3Uncles': raw['sha3Uncles'],
                    'logsBloom': raw['logsBloom'],
                    'transactionsRoot': raw['transactionsRoot'],
                    'stateRoot': raw['stateRoot'],
                    'receiptsRoot': raw['receiptsRoot'],
                    'miner': raw['miner'],
                    'gasUsed': int(raw['gasUsed'], 0),
                    'gasLimit': int(raw['gasLimit'], 0),
                    'size': int(raw['size'], 0),
                    'timestamp': int(raw['timestamp'], 0),
                    'extraData': raw['extraData']
                },
                'transactions': transactions,
                'logs': [],
            })

        for raw in response[-1]:
            log: Log = {
                'blockNumber': int(raw['blockNumber'], 0),
                'logIndex': int(raw['logIndex'], 0),
                'transactionIndex': int(raw['transactionIndex'], 0),
                'address': raw['address'],
                'data': raw['data'],
                'topic0': raw['topics'][0],
                'topic1': raw['topics'][1],
                'topic2': raw['topics'][2],
                'topic3': raw['topics'][3]
            }
            blocks[log['blockNumber'] - from_block]['logs'].append(log)

        return blocks


    def _schedule_strides(self):
        while (len(self._strides) < self._concurrency) and not self._is_finished() and (self._dist() > 0):
            from_block = self._height + 1
            stride_size = min(self._stride_size, self._dist())
            if self._end is not None:
                stride_size = min(stride_size, self._end - self._height)
            to_block = self._height + stride_size
            task = asyncio.create_task(self._fetch_stride(from_block, to_block))
            self._strides.append(task)
            self._height = to_block

    def _dist(self) -> int:
        return self._chain_height - self._height

    def _is_finished(self) -> bool:
        if self._end is not None:
            return self._height >= self._end
        return False
