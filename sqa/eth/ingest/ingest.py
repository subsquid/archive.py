import asyncio
import logging
from functools import cached_property
from typing import Optional, AsyncIterator, Literal, Iterable, Coroutine

from sqa.eth.ingest.model import Block, Log, Receipt, DebugFrame, DebugFrameResult, \
    DebugStateDiffResult, TraceTransactionReplay, Transaction
from sqa.util.rpc import RpcClient
from sqa.eth.ingest.util import qty2int, get_tx_status_from_traces, logs_bloom, \
    transactions_root, get_polygon_bor_tx_hash, recover_tx_sender
from sqa.eth.ingest.moonbase import fix_and_exclude_invalid_moonbase_blocks, is_moonbase_traceless


LOG = logging.getLogger(__name__)


class Ingest:
    def __init__(
        self,
        rpc: RpcClient,
        finality_confirmation: int,
        genesis_block: int = 0,
        from_block: int = 0,
        to_block: Optional[int] = None,
        with_receipts: bool = False,
        with_traces: bool = False,
        with_statediffs: bool = False,
        use_trace_api: bool = False,
        use_debug_api_for_statediffs: bool = False,
        debug_api_trace_config_timeout: Optional[str] = None,
        validate_tx_root: bool = False,
        validate_tx_type: bool = False,
        validate_tx_sender: bool = False,
        validate_logs_bloom: bool = False,
        polygon_based: bool = False,
    ):
        self._rpc = rpc
        self._finality_confirmation = finality_confirmation
        self._with_receipts = with_receipts
        self._with_traces = with_traces
        self._with_statediffs = with_statediffs
        self._use_trace_api = use_trace_api
        self._use_debug_api_for_statediffs = use_debug_api_for_statediffs
        self._debug_api_trace_config_timeout = debug_api_trace_config_timeout
        self._validate_tx_root = validate_tx_root
        self._validate_tx_type = validate_tx_type
        self._validate_tx_sender = validate_tx_sender
        self._validate_logs_bloom = validate_logs_bloom
        self._polygon_based = polygon_based
        self._height = from_block - 1
        self._genesis = genesis_block
        self._end = to_block
        self._chain_height = 0
        self._strides = []
        self._stride_size = 20
        self._closed = False
        self._running = False
        self._is_arbitrum_one = False
        self._is_moonbeam = False
        self._is_moonriver = False
        self._is_moonbase = False
        self._is_polygon = False
        self._is_polygon_testnet = False
        self._is_optimism = False
        self._is_astar = False
        self._is_skale_nebula = False
        self._is_bitfinity_mainnet = False

    async def loop(self) -> AsyncIterator[list[Block]]:
        assert not self._running
        self._running = True
        await self._detect_special_chains()
        while not self._closed and not self._is_finished() or len(self._strides):
            try:
                stride = self._strides.pop(0)
            except IndexError:
                await self._wait_chain()
                self._schedule_strides()
            else:
                blocks = await stride
                self._schedule_strides()
                yield blocks

    def close(self):
        self._closed = True
        self._rpc.close()

    async def _detect_special_chains(self) -> None:
        genesis: Block = await self._rpc.call('eth_getBlockByNumber', [hex(self._genesis), False])
        genesis_hash = genesis['hash']
        self._is_arbitrum_one = genesis_hash == '0x7ee576b35482195fc49205cec9af72ce14f003b9ae69f6ba0faef4514be8b442'
        self._is_moonbeam = genesis_hash == '0x7e6b3bbed86828a558271c9c9f62354b1d8b5aa15ff85fd6f1e7cbe9af9dde7e'
        self._is_moonriver = genesis_hash == '0xce24348303f7a60c4d2d3c82adddf55ca57af89cd9e2cd4b863906ef53b89b3c'
        self._is_moonbase = genesis_hash == '0x33638dde636f9264b6472b9d976d58e757fe88badac53f204f3f530ecc5aacfa'
        self._is_polygon = genesis_hash == '0xa9c28ce2141b56c474f1dc504bee9b01eb1bd7d1a507580d5519d4437a97de1b'
        self._is_polygon_testnet = genesis_hash == '0x7b66506a9ebdbf30d32b43c5f15a3b1216269a1ec3a75aa3182b86176a2b1ca7'
        self._is_optimism = genesis_hash == '0x7ca38a1916c42007829c55e69d3e9a73265554b586a499015373241b8a3fa48b'
        self._is_astar = genesis_hash == '0x0d28a86ac0fe37871285bd1dac45d83a4b3833e01a37571a1ac4f0a44c64cdc2'
        self._is_skale_nebula = genesis_hash == '0x28e07f346c28a837dfd2897ce70c8500de6e67ddbc33cb5b9cd720fff4aeb598'
        self._is_bitfinity_mainnet = genesis_hash == '0xc2fca73be73731907eec1890d851a2b64fe23616766586f0ee572b3a152eee81'

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
        hex_height = await self._rpc.call('eth_blockNumber', [])
        height = int(hex_height, 0)
        return max(height - self._finality_confirmation, 0)

    async def _fetch_stride(self, from_block: int, to_block: int) -> list[Block]:
        extra = {'first_block': from_block, 'last_block': to_block}

        LOG.debug('fetching new stride', extra=extra)

        blocks = await self._fetch_blocks(from_block, to_block)

        await _run_subtasks(self._stride_subtasks(blocks))

        if self._with_traces and not self._with_receipts:
            await _run_subtasks(
                self._fetch_single_tx_receipt(tx)
                for tx in _txs_with_missing_status(blocks)
            )

        LOG.debug('stride is ready', extra=extra)
        return blocks

    def _stride_subtasks(self, blocks: list[Block]):
        if self._is_moonbase:
            blocks = fix_and_exclude_invalid_moonbase_blocks(blocks, self._with_receipts, self._with_traces)

        if self._with_receipts:
            yield self._fetch_receipts(blocks)
        else:
            yield self._fetch_logs(blocks)

        if self._trace_tracers:
            for block in blocks:
                if self._is_arbitrum_one:
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
                        if self._is_moonbase and qty2int(block['number']) < 610936:
                            # traces on moonbase aren't available before moonbase@400 runtime upgrade
                            pass
                        else:
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

        if self._with_statediffs and not self._is_arbitrum_one:
            tracers.append('stateDiff')

        return tracers

    async def _fetch_blocks(self, from_block: int, to_block: int) -> list[Block]:
        blocks: list[Block] = await self._rpc.batch_call(
            [
                ('eth_getBlockByNumber', [hex(i), True])
                for i in range(from_block, to_block + 1)
            ],
            priority=from_block
        )

        if self._is_skale_nebula:
            for block in blocks:
                for tx in block['transactions']:
                    tx['type'] = '0x0'
        if self._is_bitfinity_mainnet:
            for block in blocks:
                for tx in block['transactions']:
                    if tx.get('type') is None:
                        if 'maxPriorityFeePerGas' in tx:
                            tx['type'] = '0x2'
                        else:
                            tx['type'] = '0x0'

        if self._validate_tx_root:
            for block in blocks:
                if self._polygon_based:
                    state_sync_tx_hash = get_polygon_bor_tx_hash(qty2int(block['number']), block['hash'])
                    txs = [tx for tx in block['transactions'] if tx['hash'] != state_sync_tx_hash]
                    assert block['transactionsRoot'] == transactions_root(txs)
                else:
                    assert block['transactionsRoot'] == transactions_root(block['transactions'])

        if self._validate_tx_sender:
            for block in blocks:
                for tx in block['transactions']:
                    assert tx['from'] == recover_tx_sender(tx)

        return blocks

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

        tx_by_index: dict[str, dict[str, Transaction]] = {}
        for block in blocks:
            tx_by_index[block['hash']] = {tx['transactionIndex']: tx for tx in block['transactions']}

        logs_by_hash: dict[str, list[Log]] = {}
        for log in logs:
            block_logs = logs_by_hash.setdefault(log['blockHash'], [])
            tx = tx_by_index[log['blockHash']][log['transactionIndex']]
            if self._is_polygon and _is_polygon_precompiled(tx):
                continue
            block_logs.append(log)

        for block in blocks:
            block_logs = logs_by_hash.get(block['hash'], [])

            if self._validate_logs_bloom:
                if self._polygon_based:
                    state_sync_tx_hash = get_polygon_bor_tx_hash(qty2int(block['number']), block['hash'])
                    logs = [log for log in block_logs if log['transactionHash'] != state_sync_tx_hash]
                    assert block['logsBloom'] == logs_bloom(logs)
                else:
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

        tx_by_index: dict[str, dict[str, Transaction]] = {}
        for block in blocks:
            tx_by_index[block['hash']] = {tx['transactionIndex']: tx for tx in block['transactions']}

        receipts_map: dict[str, Receipt] = {}
        logs_by_hash: dict[str, list[Log]] = {}
        for r in receipts:
            receipts_map[r['transactionHash']] = r
            block_logs = logs_by_hash.setdefault(r['blockHash'], [])

            if self._is_arbitrum_one and r['transactionHash'] == '0x1d76d3d13e9f8cc713d484b0de58edd279c4c62e46e963899aec28eb648b5800':
                continue
            if self._is_skale_nebula:
                r['type'] = '0x0'
            elif self._is_bitfinity_mainnet:
                if r.get('type') is None:
                    tx = tx_by_index[r['blockHash']][r['transactionIndex']]
                    assert r['transactionHash'] == tx['hash']
                    r['type'] = tx['type']
            elif self._validate_tx_type:
                assert r.get('type') is not None

            try:
                tx = tx_by_index[r['blockHash']][r['transactionIndex']]
            except KeyError as e:
                if self._is_optimism:
                    # optimism doesn't provide receipts for duplicated transactions
                    # so we just skip such transactions
                    continue
                if self._is_moonbase and 2529736 <= qty2int(r['blockNumber']) <= 3069634:
                    # receipts aren't accessible for duplicated transactions
                    # https://github.com/moonbeam-foundation/moonbeam/pull/1790
                    continue
                else:
                    raise e

            if (self._is_polygon or self._is_polygon_testnet) and _is_polygon_precompiled(tx):
                continue

            for log in r['logs']:
                tx = tx_by_index[log['blockHash']][log['transactionIndex']]
                assert log['transactionHash'] == tx['hash']
                block_logs.append(log)

        for block in blocks:
            if self._is_moonriver and qty2int(block['number']) == 2077599:
                _fix_frontier_duplication_bug(block, receipts_map, logs_by_hash)

            if self._is_moonriver and qty2int(block['number']) == 2077600:
                _fix_moonriver_2077600(block)

            if self._is_astar and qty2int(block['number']) == 995595:
                _fix_frontier_duplication_bug(block, receipts_map, logs_by_hash)

            if self._is_astar and qty2int(block['number']) == 995596:
                _fix_astar_995596(block)

            block_logs = logs_by_hash.get(block['hash'], [])

            if self._validate_logs_bloom:
                if self._polygon_based:
                    state_sync_tx_hash = get_polygon_bor_tx_hash(qty2int(block['number']), block['hash'])
                    logs = [log for log in block_logs if log['transactionHash'] != state_sync_tx_hash]
                    assert block['logsBloom'] == logs_bloom(logs)
                else:
                    assert block['logsBloom'] == logs_bloom(block_logs)

            for tx in block['transactions']:
                if self._is_arbitrum_one and tx['hash'] == '0x1d76d3d13e9f8cc713d484b0de58edd279c4c62e46e963899aec28eb648b5800' and block['number'] == hex(4527955):
                    continue
                if self._is_moonbase and 2529736 <= qty2int(block['number']) <= 3069634 and receipts_map[tx['hash']]['blockHash'] != block['hash']:
                    continue
                if self._is_optimism and receipts_map[tx['hash']]['blockHash'] != block['hash']:
                    continue
                tx['receipt_'] = receipts_map[tx['hash']]

    async def _fetch_single_tx_receipt(self, tx: Transaction) -> None:
        block_number = qty2int(tx['blockNumber'])

        receipt: Receipt = await self._rpc.call(
            'eth_getTransactionReceipt',
            [tx['hash']],
            priority=block_number
        )

        if self._is_skale_nebula:
            receipt['type'] = '0x0'
        if self._is_bitfinity_mainnet:
            if receipt.get('type') is None:
                tx: Transaction = await self._rpc.call(
                    'eth_getTransactionByHash',
                    [tx['hash']],
                    priority=block_number
                )
                if 'maxPriorityFeePerGas' in tx:
                    receipt['type'] = '0x2'
                else:
                    receipt['type'] = '0x0'

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
                },
                'timeout': self._debug_api_trace_config_timeout,
            },
        ], priority=block_number, validate_result=_validate_debug_trace)

        if self._is_moonriver and qty2int(block['number']) == 2077600:
            _fix_moonriver_2077600(block)

        transactions = block['transactions']
        if self._polygon_based:
            transactions = [
                tx for tx in transactions
                if tx['hash'] != get_polygon_bor_tx_hash(block_number, block['hash'])
            ]
        if self._is_polygon or self._is_polygon_testnet:
            transactions = [tx for tx in transactions if not _is_polygon_precompiled(tx)]
        if self._is_moonbase:
            transactions = [tx for tx in transactions if not is_moonbase_traceless(tx, block)]
        if self._is_moonbeam and len(traces) > len(transactions):
            _delete_extra_traces(transactions, traces)

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
                },
                'timeout': self._debug_api_trace_config_timeout,
            }
        ], priority=block_number, validate_result=_validate_debug_statediffs)

        transactions = block['transactions']
        if self._polygon_based:
            transactions = [
                tx for tx in transactions
                if tx['hash'] != get_polygon_bor_tx_hash(block_number, block['hash'])
            ]
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


def _txs_with_missing_status(blocks: list[Block]) -> Iterable[Transaction]:
    for block in blocks:
        for tx in block['transactions']:
            if not get_tx_status_from_traces(tx):
                yield tx


async def _run_subtasks(coros: Iterable[Coroutine]) -> None:
    subtasks = [asyncio.create_task(coro) for coro in coros]
    for task in subtasks:
        await task


def _is_polygon_precompiled(tx: Transaction):
    address = '0x0000000000000000000000000000000000000000'
    return tx['from'] == address and tx['to'] == address


def _fix_frontier_duplication_bug(block: Block, receipts_map: dict[str, Receipt], logs_by_hash: dict[str, list[Log]]):
    block_logs = []
    for tx in block['transactions']:
        receipt = receipts_map[tx['hash']]
        receipt['blockHash'] = block['hash']
        receipt['blockNumber'] = block['number']
        for log in receipt['logs']:
            log['blockHash'] = block['hash']
            log['blockNumber'] = block['number']
            block_logs.append(log)
    logs_by_hash[block['hash']] = block_logs


def _fix_moonriver_2077600(block: Block):
    if len(block['transactions']) == 31:
        block['transactions'] = block['transactions'][23:]
    else:
        # transactions were already cut in a different method
        assert len(block['transactions']) == 8


def _fix_astar_995596(block: Block):
    assert len(block['transactions']) == 123
    block['transactions'] = block['transactions'][58:]


def _validate_debug_trace(result):
    for trace in result:
        if trace.get('error') == 'execution timeout':
            return False
        if error := trace.get('error'):
            if isinstance(error, dict):
                return False
    return True

def _validate_debug_statediffs(result):
    for diff in result:
        if error := diff.get('error'):
            if isinstance(error, dict):
                return False
    return True


def _is_trace_from_tx(trace: DebugFrame, tx: Transaction):
    if trace['from'] != tx['from']:
        return False
    if trace['to'] != tx['to']:
        return False
    return trace['value'] == tx['value']

def _delete_extra_traces(transactions: list[Transaction], traces: list[DebugFrame]):
    for idx, tx in enumerate(transactions):
        while True:
            trace = traces[idx]
            if _is_trace_from_tx(trace, tx):
                break
            else:
                del traces[idx]
