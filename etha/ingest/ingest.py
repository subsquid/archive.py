import asyncio
from itertools import groupby
from typing import Optional, AsyncIterator

from etha.ingest.model import Block, Log, Receipt
from etha.ingest.rpc import RpcClient


class Ingest:
    def __init__(
            self,
            rpc: RpcClient,
            finality_offset: int = 10,
            from_block: int = 0,
            to_block: Optional[int] = None,
            with_receipts: bool = False,
            with_traces: bool = False
    ):
        self._rpc = rpc
        self._finality_offset = finality_offset
        self._with_receipts = with_receipts
        self._with_traces = with_traces
        self._height = from_block - 1
        self._end = to_block
        self._chain_height = 0
        self._strides = []
        self._stride_size = 10

    async def loop(self) -> AsyncIterator[list[Block]]:
        while not self._is_finished() or len(self._strides):
            try:
                stride = self._strides.pop(0)
            except IndexError:
                await self._wait_chain()
                self._schedule_strides()
            else:
                blocks = await stride
                self._schedule_strides()
                yield blocks

    def _schedule_strides(self):
        while len(self._strides) < 2 and not self._is_finished() and self._dist() > 0:
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
        if self._end is None:
            return False
        else:
            return self._height >= self._end

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
        hex_height = await self._rpc.call('eth_blockNumber')
        height = int(hex_height, 0)
        return max(height - self._finality_offset, 0)

    async def _fetch_stride(self, from_block: int, to_block: int) -> list[Block]:
        if self._with_receipts:
            logs_future = None
        else:
            logs_future = self._rpc.call('eth_getLogs', [{
                'fromBlock': hex(from_block),
                'toBlock': hex(to_block)
            }], priority=from_block)

        block_tasks = [asyncio.create_task(self._fetch_block(i)) for i in range(from_block, to_block + 1)]
        await asyncio.wait(block_tasks)
        blocks = [t.result() for t in block_tasks]

        if logs_future:
            logs: list[Log] = await logs_future
            logs.sort(key=lambda b: b['blockNumber'])
            logs_by_block = {k: list(it) for k, it in groupby(logs, key=lambda b: b['blockNumber'])}

            for block in blocks:
                block_hash = block['hash']
                block_logs = logs_by_block.get(block['number'], [])
                for rec in block_logs:
                    assert rec['blockHash'] == block_hash
                block['logs_'] = block_logs

        return blocks

    async def _fetch_block(self, height: int) -> Block:
        if self._with_traces:
            trace_future = self._rpc.call('trace_block', [hex(height)], priority=height)
        else:
            trace_future = None

        block: Block = await self._rpc.call('eth_getBlockByNumber', [hex(height), True], priority=height)

        if self._with_receipts and block['transactions']:
            logs: list[Log] = []

            receipt_futures = [
                self._rpc.call('eth_getTransactionReceipt', [tx['hash']], priority=height)
                for tx in block['transactions']
            ]

            await asyncio.wait(receipt_futures)

            for rf, tx in zip(receipt_futures, block['transactions']):
                receipt: Receipt = rf.result()
                tx['receipt_'] = receipt
                logs.extend(receipt['logs'])

            block['logs_'] = logs

        if trace_future:
            block['trace_'] = await trace_future

        return block
