import asyncio
import logging
from typing import Optional, AsyncIterator

from etha.ingest.model import Block, Log, Receipt, CallFrameResult, StateDiffResult
from etha.ingest.rpc import RpcClient
from etha.ingest.util import trim_hash, qty2int


LOG = logging.getLogger(__name__)


class Ingest:
    def __init__(
        self,
        rpc: RpcClient,
        finality_offset: int = 10,
        from_block: int = 0,
        to_block: Optional[int] = None,
        last_hash: Optional[str] = None,
        with_receipts: bool = False,
        with_traces: bool = False
    ):
        self._rpc = rpc
        self._finality_offset = finality_offset
        self._with_receipts = with_receipts
        self._with_traces = with_traces
        self._last_hash = last_hash
        self._height = from_block - 1
        self._end = to_block
        self._chain_height = 0
        self._strides = []
        self._stride_size = 20

    async def loop(self) -> AsyncIterator[list[Block]]:
        while not self._is_finished() or len(self._strides):
            try:
                stride = self._strides.pop(0)
            except IndexError:
                await self._wait_chain()
                self._schedule_strides()
            else:
                blocks = await stride
                self._validate_blocks(blocks)
                self._schedule_strides()
                yield blocks

    def _schedule_strides(self):
        while len(self._strides) < max(1, min(10, self._rpc.get_total_capacity())) \
                and not self._is_finished() \
                and self._dist() > 0:
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
        extra = {'first_block': from_block, 'last_block': to_block}

        LOG.debug('fetching new stride', extra=extra)

        blocks: list[Block] = await self._rpc.batch_call(
            [
                ('eth_getBlockByNumber', [hex(i), True])
                for i in range(from_block, to_block + 1)
            ],
            priority=from_block
        )

        subtasks = [
            asyncio.create_task(coro)
            for coro in self._stride_subtasks(blocks)
        ]

        for sub in subtasks:
            await sub

        LOG.debug('stride is ready', extra=extra)
        return blocks

    def _stride_subtasks(self, blocks: list[Block]):
        if self._with_receipts:
            yield self._fetch_receipts(blocks)
        else:
            yield self._fetch_logs(blocks)

        if self._with_traces:
            for block in blocks:
                yield self._fetch_call_trace(block)
                yield self._fetch_state_diff(block)

    async def _fetch_logs(self, blocks: list[Block]) -> None:
        priority = qty2int(blocks[0]['number'])

        logs: list[Log] = await self._rpc.call(
            'eth_getLogs',
            [{
                'fromBlock': blocks[0]['number'],
                'toBlock': blocks[-1]['number']
            }],
            priority=priority
        )

        block_map = {}
        for block in blocks:
            block['logs_'] = []
            block_map[block['hash']] = block

        for log in logs:
            block = block_map[log['blockHash']]
            block['logs_'].append(log)

    async def _fetch_receipts(self, blocks: list[Block]) -> None:
        priority = qty2int(blocks[0]['number'])

        receipts: list[Receipt] = await self._rpc.batch_call(
            [
                ('eth_getTransactionReceipt', [tx['hash']])
                for b in blocks
                for tx in b['transactions']
            ],
            priority=priority
        )

        receipts_map = {r['transactionHash']: r for r in receipts}

        for block in blocks:
            logs = block['logs_'] = []
            for tx in block['transactions']:
                r = receipts_map[tx['hash']]
                logs.extend(r['logs'])
                tx['receipt_'] = r

    async def _fetch_call_trace(self, block: Block) -> None:
        priority = qty2int(block['number'])

        traces: list[CallFrameResult] = await self._rpc.call('debug_traceBlockByHash', [
            block['hash'],
            {
                'tracer': 'callTracer',
                'tracerConfig': {
                    'onlyTopCall': False,
                    'withLog': True
                }
            }
        ], priority=priority)

        transactions = block['transactions']
        assert len(transactions) == len(traces)
        for tx, trace in zip(transactions, traces):
            tx['callTrace_'] = trace['result']

    async def _fetch_state_diff(self, block: Block) -> None:
        priority = qty2int(block['number'])

        diffs: list[StateDiffResult] = await self._rpc.call('debug_traceBlockByHash', [
            block['hash'],
            {
                'tracer': 'prestateTracer',
                'tracerConfig': {
                    'onlyTopCall': False,  # Incorrect, but required by Alchemy endpoints
                    'diffMode': True
                }
            }
        ], priority=priority)

        transactions = block['transactions']
        assert len(transactions) == len(diffs)
        for tx, diff in zip(transactions, diffs):
            tx['stateDiff_'] = diff['result']

    def _validate_blocks(self, blocks: list[Block]):
        for block in blocks:
            block_parent_hash = trim_hash(block['parentHash'])
            block_hash = trim_hash(block['hash'])

            if self._last_hash and self._last_hash != block_parent_hash:
                raise Exception(f'broken chain: block {block_hash} is not a direct child of {self._last_hash}')

            self._last_hash = block_hash
