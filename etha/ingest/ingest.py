import asyncio
import logging
from itertools import groupby
from typing import Optional, AsyncIterator

from etha.ingest.model import Block, Log, Receipt, TransactionReplay
from etha.ingest.rpc import RpcClient, RpcBatchCall
from etha.ingest.util import trim_hash, qty2int


LOG = logging.getLogger(__name__)


class Ingest:
    def __init__(
        self,
        rpc: RpcClient,
        finality_offset: int = 10,
        first_block: int = 0,
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
        self._first_block = first_block
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
        while len(self._strides) < max(1, min(50, self._rpc.get_total_capacity())) \
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

        block_batch: RpcBatchCall = [
            ('eth_getBlockByNumber', [hex(i), True])
            for i in range(from_block, to_block + 1)
        ]

        if not self._with_receipts:
            block_batch.append(
                ('eth_getLogs', [{
                    'fromBlock': hex(from_block),
                    'toBlock': hex(to_block)
                }])
            )

        if self._with_traces:
            trace_batch = [
                ('trace_replayBlockTransactions', [hex(i), ['trace', 'stateDiff']])
                for i in range(from_block, to_block + 1)
            ]
        else:
            trace_batch = []

        trace_future = self._rpc.batch_call(trace_batch, priority=from_block)

        block_batch_response = await self._rpc.batch_call(block_batch, priority=from_block)
        blocks: list[Block] = block_batch_response[:(to_block - from_block + 1)]

        receipts: list[Receipt]
        logs: list[Log]
        if self._with_receipts:
            receipts = await self._rpc.batch_call(
                [
                    ('eth_getTransactionReceipt', [tx['hash']])
                    for b in blocks
                    for tx in b['transactions']
                ],
                priority=from_block
            )
            logs = []
            for r in receipts:
                logs.extend(r['logs'])
        else:
            receipts = []
            logs = block_batch_response[-1]

        replays: list[list[TransactionReplay]] = await trace_future

        logs.sort(key=lambda rec: rec['blockNumber'])
        logs_by_block = {k: list(it) for k, it in groupby(logs, key=lambda b: b['blockNumber'])}

        receipts_by_tx = {r['transactionHash']: r for r in receipts}

        for i, block in enumerate(blocks):
            block_number = block['number']
            block_hash = block['hash']

            block_logs = logs_by_block.get(block_number, [])
            for item in block_logs:
                assert item['blockHash'] == block_hash
            block['logs_'] = block_logs

            if self._with_traces:
                replay_by_tx = {replay['transactionHash']: replay for replay in replays[i]}
                for tx in block['transactions']:
                    tx['replay_'] = replay_by_tx[tx['hash']]

            if self._with_receipts:
                for tx in block['transactions']:
                    tx['receipt_'] = receipts_by_tx[tx['hash']]

        LOG.debug('stride is ready', extra=extra)

        return blocks

    def _validate_blocks(self, blocks: list[Block]):
        for block in blocks:
            parent_hash = trim_hash(block['parentHash'])
            if self._last_hash == parent_hash or qty2int(block['number']) == self._first_block:
                self._last_hash = trim_hash(block['hash'])
            else:
                block_number = qty2int(block['number'])
                raise InconsistencyError(f"block {block_number} doesn't match hash from the parent block: '{parent_hash}' != '{self._last_hash}'")


class InconsistencyError(Exception):
    pass
