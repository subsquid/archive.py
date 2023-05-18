import asyncio
import logging
from functools import cached_property
from typing import Optional, AsyncIterator, Literal, Iterable, Coroutine

from etha.ingest.model import Block, Log, Receipt, DebugFrame, DebugFrameResult, \
    DebugStateDiffResult, TraceTransactionReplay, Transaction
from etha.ingest.rpc import RpcClient
from etha.ingest.util import short_hash, qty2int, get_tx_status_from_traces, logs_bloom


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
        with_traces: bool = False,
        with_statediffs: bool = False,
        use_trace_api: bool = False,
        use_debug_api_for_statediffs: bool = False,
        arbitrum: bool = False
    ):
        self._rpc = rpc
        self._finality_offset = finality_offset
        self._with_receipts = with_receipts
        self._with_traces = with_traces
        self._with_statediffs = with_statediffs
        self._use_trace_api = use_trace_api
        self._use_debug_api_for_statediffs = use_debug_api_for_statediffs
        self._arbitrum = arbitrum
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

        await _run_subtasks(self._stride_subtasks(blocks))

        if self._with_traces and not self._with_receipts:
            await _run_subtasks(
                self._fetch_single_tx_receipt(tx)
                for tx in _txs_with_missing_status(blocks)
            )

        LOG.debug('stride is ready', extra=extra)
        return blocks

    def _stride_subtasks(self, blocks: list[Block]):
        if self._with_receipts:
            yield self._fetch_receipts(blocks)
        else:
            yield self._fetch_logs(blocks)

        if self._trace_tracers:
            for block in blocks:
                if self._arbitrum:
                    bn = qty2int(block['number'])
                    if 1 < bn < 22207815:
                        yield self._fetch_trace_replay(
                            block,
                            self._trace_tracers,
                            method='arbtrace_replayBlockTransactions'
                        )
                    elif bn >= 22207818:
                        yield self._fetch_debug_call_trace(block)
                elif self._use_trace_api:
                    yield self._fetch_trace_replay(block, self._trace_tracers)
                else:
                    if self._with_traces:
                        yield self._fetch_debug_call_trace(block)
                    if self._with_statediffs:
                        if self._use_debug_api_for_statediffs:
                            yield self._fetch_debug_state_diff(block)
                        else:
                            yield self._fetch_trace_replay(block, ['stateDiff'])

    @cached_property
    def _trace_tracers(self) -> list[Literal['trace', 'stateDiff']]:
        tracers: list[Literal['trace', 'stateDiff']] = []

        if self._with_traces:
            tracers.append('trace')

        if self._with_statediffs and not self._arbitrum:
            tracers.append('stateDiff')

        return tracers

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

        logs_by_hash: dict[str, list[Log]] = {}
        for log in logs:
            block_logs = logs_by_hash.setdefault(log['blockHash'], [])
            block_logs.append(log)

        for block in blocks:
            block_logs = logs_by_hash.get(block['hash'], [])
            assert block['logsBloom'] == logs_bloom(block_logs)
            block['logs_'] = block_logs

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

        receipts_map: dict[str, Receipt] = {}
        logs_by_hash: dict[str, list[Log]] = {}
        for r in receipts:
            receipts_map[r['transactionHash']] = r
            block_logs = logs_by_hash.setdefault(r['blockHash'], [])
            for log in r['logs']:
                block_logs.append(log)

        for block in blocks:
            block_logs = logs_by_hash.get(block['hash'], [])
            assert block['logsBloom'] == logs_bloom(block_logs)
            for tx in block['transactions']:
                tx['receipt_'] = receipts_map[tx['hash']]

    async def _fetch_single_tx_receipt(self, tx: Transaction) -> None:
        block_number = qty2int(tx['blockNumber'])

        receipt: Receipt = await self._rpc.call(
            'eth_getTransactionReceipt',
            [tx['hash']],
            priority=block_number
        )

        assert receipt['transactionHash'] == tx['hash']
        tx['receipt_'] = receipt

    async def _fetch_debug_call_trace(self, block: Block) -> None:
        block_number = qty2int(block['number'])
        if block_number == 0:
            return

        traces: list[DebugFrameResult | DebugFrame] = await self._rpc.call('debug_traceBlockByHash', [
            block['hash'],
            {
                'tracer': 'callTracer',
                'tracerConfig': {
                    'onlyTopCall': False,
                    'withLog': True
                }
            }
        ], priority=block_number)

        transactions = block['transactions']
        assert len(transactions) == len(traces)
        for tx, trace in zip(transactions, traces):
            if 'result' not in trace:
                trace = {'result': trace}
            tx['debugFrame_'] = trace

    async def _fetch_debug_state_diff(self, block: Block) -> None:
        block_number = qty2int(block['number'])
        if block_number == 0:
            return

        diffs: list[DebugStateDiffResult] = await self._rpc.call('debug_traceBlockByHash', [
            block['hash'],
            {
                'tracer': 'prestateTracer',
                'tracerConfig': {
                    'onlyTopCall': False,  # Incorrect, but required by Alchemy endpoints
                    'diffMode': True
                }
            }
        ], priority=block_number)

        transactions = block['transactions']
        assert len(transactions) == len(diffs)
        for tx, diff in zip(transactions, diffs):
            assert 'result' in diff
            tx['debugStateDiff_'] = diff

    async def _fetch_trace_replay(self,
                                  block: Block,
                                  tracers: list[Literal['trace', 'stateDiff']],
                                  method: str = 'trace_replayBlockTransactions'
                                  ) -> None:
        block_number = qty2int(block['number'])
        if block_number == 0:
            return

        replays: list[TraceTransactionReplay] = await self._rpc.call(
            method,
            [block['number'], tracers],
            priority=block_number
        )

        unassigned_replays = []

        for i, rep in enumerate(replays):
            rep['index_'] = i
            if 'transactionHash' in rep:
                pass
            else:
                tx_hash = None
                for trace in (rep['trace'] or []):
                    assert tx_hash is None or tx_hash == trace['transactionHash']
                    tx_hash = trace['transactionHash']
                rep['transactionHash'] = tx_hash
                if not tx_hash:
                    unassigned_replays.append(rep)

        by_tx = {rep['transactionHash']: rep for rep in replays if rep['transactionHash']}
        used = 0
        for tx in block['transactions']:
            if rep := by_tx.get(tx['hash']):
                tx['traceReplay_'] = rep
                used += 1
        assert used == len(by_tx)

        if unassigned_replays:
            block['unknownTraceReplays_'] = unassigned_replays

    def _validate_blocks(self, blocks: list[Block]):
        for block in blocks:
            block_parent_hash = short_hash(block['parentHash'])
            block_hash = short_hash(block['hash'])

            if self._last_hash and self._last_hash != block_parent_hash:
                raise Exception(f'broken chain: block {block_hash} is not a direct child of {self._last_hash}')

            self._last_hash = block_hash


def _txs_with_missing_status(blocks: list[Block]) -> Iterable[Transaction]:
    for block in blocks:
        for tx in block['transactions']:
            if not get_tx_status_from_traces(tx):
                yield tx


async def _run_subtasks(coros: Iterable[Coroutine]) -> None:
    subtasks = [asyncio.create_task(coro) for coro in coros]
    for task in subtasks:
        await task
