import asyncio
import logging
from functools import cached_property
from typing import Optional, AsyncIterator, Literal, Iterable, Coroutine

from etha.ingest.model import Block, Log, Receipt, DebugFrame, DebugFrameResult, \
    DebugStateDiffResult, TraceTransactionReplay, Transaction
from etha.ingest.rpc import RpcClient
from etha.ingest.util import qty2int, get_tx_status_from_traces, logs_bloom


LOG = logging.getLogger(__name__)


class Ingest:
    def __init__(
        self,
        rpc: RpcClient,
        finality_confirmation: int,
        from_block: int = 0,
        to_block: Optional[int] = None,
        with_receipts: bool = False,
        with_traces: bool = False,
        with_statediffs: bool = False,
        use_trace_api: bool = False,
        use_debug_api_for_statediffs: bool = False
    ):
        self._rpc = rpc
        self._finality_confirmation = finality_confirmation
        self._with_receipts = with_receipts
        self._with_traces = with_traces
        self._with_statediffs = with_statediffs
        self._use_trace_api = use_trace_api
        self._use_debug_api_for_statediffs = use_debug_api_for_statediffs
        self._height = from_block - 1
        self._end = to_block
        self._chain_height = 0
        self._strides = []
        self._stride_size = 20
        self._closed = False
        self._running = False
        self._is_arbitrum_one = False
        self._is_moonriver = False
        self._is_moonbase = False
        self._is_polygon = False

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
        genesis: Block = await self._rpc.call('eth_getBlockByNumber', ['0x0', False])
        genesis_hash = genesis['hash']
        self._is_arbitrum_one = genesis_hash == '0x7ee576b35482195fc49205cec9af72ce14f003b9ae69f6ba0faef4514be8b442'
        self._is_moonriver = genesis_hash == '0xce24348303f7a60c4d2d3c82adddf55ca57af89cd9e2cd4b863906ef53b89b3c'
        self._is_moonbase = genesis_hash == '0x33638dde636f9264b6472b9d976d58e757fe88badac53f204f3f530ecc5aacfa'
        self._is_polygon = genesis_hash == '0xa9c28ce2141b56c474f1dc504bee9b01eb1bd7d1a507580d5519d4437a97de1b'

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
        return max(height - self._finality_confirmation, 0)

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
        if self._is_moonbase:
            blocks = _fix_and_exclude_invalid_moonbase_blocks(blocks, self._with_receipts, self._with_traces)

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
            tx = tx_by_index[r['blockHash']][r['transactionIndex']]
            if self._is_polygon and _is_polygon_precompiled(tx):
                continue
            for log in r['logs']:
                block_logs.append(log)

        for block in blocks:
            if self._is_moonriver and qty2int(block['number']) == 2077599:
                _fix_moonriver_2077599(block, receipts_map, logs_by_hash)

            if self._is_moonriver and qty2int(block['number']) == 2077600:
                _fix_moonriver_2077600(block)

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

        if self._is_moonriver and qty2int(block['number']) == 2077600:
            _fix_moonriver_2077600(block)

        transactions = block['transactions']
        if self._is_polygon:
            transactions = [tx for tx in transactions if not _is_polygon_precompiled(tx)]
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


def _fix_moonriver_2077599(block: Block, receipts_map: dict[str, Receipt], logs_by_hash: dict[str, list[Log]]):
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


def _fix_and_exclude_invalid_moonbase_blocks(blocks: list[Block], with_receipts: bool, with_traces: bool):
    blocks_ = []
    for idx, block in enumerate(blocks):
        if block['number'] == hex(2285347):
            blocks[idx] = _moonbase_2285347(with_receipts, with_traces)
        elif block['number'] == hex(2285348):
            blocks[idx] = _moonbase_2285348(with_receipts, with_traces)
        else:
            blocks_.append(block)
    return blocks_


def _moonbase_2285347(with_receipts: bool, with_traces: bool) -> Block:
    block: Block = {'author': '0x1610fce4655f77d11184fbf31e497b927326ac90', 'baseFeePerGas': '0x3b9aca00', 'difficulty': '0x0', 'extraData': '0x', 'gasLimit': '0xe4e1c0', 'gasUsed': '0x2164e', 'hash': '0x1839f342b4e0163e747e67705b4a7590067ce338bee8f6ea07a24a62aa5b3831', 'logsBloom': '0x00000000000000080000000000000000400040000000000000008000000000000000000000000001000000000080000000000000000000000000000001200000000000000000000000000000000000000000000000000000000000000000000000020000000000000000000000000000000000000002000000000000000000000000000000000000000000000000000000000000002000000000000000000000020000000000000000000000000000000000000000000000000000000000000000000000000000000000000000040000400000000000000001000000000000000010000000000000000000000000000000000000000000000000020020000000', 'miner': '0x1610fce4655f77d11184fbf31e497b927326ac90', 'nonce': '0x0000000000000000', 'number': '0x22df23', 'parentHash': '0x219abbbb04da11bb278de9afc19f025fc1f7567be995cd1a1a28dc8398fa22cc', 'receiptsRoot': '0xdba9926e83c1907e92ac9cbbf55b99c074e6a63406325392c247fbc83f19697d', 'sha3Uncles': '0x1dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347', 'size': '0x53b', 'stateRoot': '0x99c64bd32a76183c5fc21d0fd0d6acd888d4e0695b6cb57a796f0659c0d37691', 'timestamp': '0x62a1ede6', 'totalDifficulty': '0x0', 'transactions': [{'accessList': None, 'blockHash': '0x1839f342b4e0163e747e67705b4a7590067ce338bee8f6ea07a24a62aa5b3831', 'blockNumber': '0x22df23', 'chainId': '0x507', 'creates': None, 'from': '0x9755f1ea7a6f6058dfc02f95bc932c3c261c1f31', 'gas': '0x5208', 'gasPrice': '0x2e90edd000', 'hash': '0x006a6843eb35ad35a9ea9a99affa8d81f1ed500253c98cc9c080d84171a0afb3', 'input': '0x', 'nonce': '0x372eb', 'publicKey': '0x66ffd4521b32aff841e5e9789fda4c8176502f78ff520dc6cfcee2041f4b7f95413464d5e7f80ffdb959493472728ccabc9155468e1c9cca9eb44f653f05e49a', 'r': '0x687cba83a3900aaa12bb08d72162afea6e4bde20a63e8209e1becb4d88d112fa', 'raw': '0xf871830372eb852e90edd000825208944e0078423a39efbc1f8b5104540ac2650a756577881bb281869ab9800080820a31a0687cba83a3900aaa12bb08d72162afea6e4bde20a63e8209e1becb4d88d112faa06675e9225efe5f76dfaf2f8a4c01fabaaf7d7e4a7bb29e8be191d5bd77e8febd', 's': '0x6675e9225efe5f76dfaf2f8a4c01fabaaf7d7e4a7bb29e8be191d5bd77e8febd', 'standardV': '0x0', 'to': '0x4e0078423a39efbc1f8b5104540ac2650a756577', 'transactionIndex': '0x0', 'type': '0x0', 'v': '0xa31', 'value': '0x1bb281869ab98000', 'receipt_': {'transactionHash': '0x006a6843eb35ad35a9ea9a99affa8d81f1ed500253c98cc9c080d84171a0afb3', 'transactionIndex': '0x0', 'blockHash': '0x1839f342b4e0163e747e67705b4a7590067ce338bee8f6ea07a24a62aa5b3831', 'from': '0x9755f1ea7a6f6058dfc02f95bc932c3c261c1f31', 'to': '0x4e0078423a39efbc1f8b5104540ac2650a756577', 'blockNumber': '0x22df23', 'cumulativeGasUsed': '0x5208', 'gasUsed': '0x5208', 'contractAddress': None, 'logs': [], 'logsBloom': '0x00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000', 'status': '0x1', 'effectiveGasPrice': '0x2e90edd000', 'type': '0x0'}, 'debugFrame_': {'result': {'from': '0x9755f1ea7a6f6058dfc02f95bc932c3c261c1f31', 'gas': '0x0', 'gasUsed': '0x5208', 'type': 'CALL', 'to': '0x4e0078423a39efbc1f8b5104540ac2650a756577', 'input': '0x', 'output': '0x', 'value': '0x1bb281869ab98000'}}}, {'accessList': None, 'blockHash': '0x1839f342b4e0163e747e67705b4a7590067ce338bee8f6ea07a24a62aa5b3831', 'blockNumber': '0x22df23', 'chainId': '0x507', 'creates': None, 'from': '0xf02d804b19b0665690f6b312691b2eb8f80cd3b8', 'gas': '0x5208', 'gasPrice': '0x2e90edd000', 'hash': '0x64c102f664eb435206ad4fcb49b526722176bcf74801c79473c3b5b2c281a243', 'input': '0x', 'nonce': '0x50846', 'publicKey': '0x6a79dbe1528444f91b06dc487a8ff1160ae2b6bbb2a5546a79c234a34a5a2ebf97694f3cc72f0f1bc51944f0c7a08271765e5f393725762e588b866e5f744cd6', 'r': '0x47a3d4fb9a0ccc8da9ca1df4e130daaee753831e4af1bc54357151298511dde6', 'raw': '0xf86d83050846852e90edd00082520894f02d804b19b0665690f6b312691b2eb8f80cd3b8843b9aca0080820a31a047a3d4fb9a0ccc8da9ca1df4e130daaee753831e4af1bc54357151298511dde6a07c792f20fdbb9ef7d400d11c591098912c62559683d8c0c05a0f4ac0e35f4ed9', 's': '0x7c792f20fdbb9ef7d400d11c591098912c62559683d8c0c05a0f4ac0e35f4ed9', 'standardV': '0x0', 'to': '0xf02d804b19b0665690f6b312691b2eb8f80cd3b8', 'transactionIndex': '0x1', 'type': '0x0', 'v': '0xa31', 'value': '0x3b9aca00', 'receipt_': {'transactionHash': '0x64c102f664eb435206ad4fcb49b526722176bcf74801c79473c3b5b2c281a243', 'transactionIndex': '0x1', 'blockHash': '0x1839f342b4e0163e747e67705b4a7590067ce338bee8f6ea07a24a62aa5b3831', 'from': '0xf02d804b19b0665690f6b312691b2eb8f80cd3b8', 'to': '0xf02d804b19b0665690f6b312691b2eb8f80cd3b8', 'blockNumber': '0x22df23', 'cumulativeGasUsed': '0xa410', 'gasUsed': '0x5208', 'contractAddress': None, 'logs': [], 'logsBloom': '0x00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000', 'status': '0x1', 'effectiveGasPrice': '0x2e90edd000', 'type': '0x0'}, 'debugFrame_': {'result': {'from': '0xf02d804b19b0665690f6b312691b2eb8f80cd3b8', 'gas': '0x0', 'gasUsed': '0x0', 'type': 'CALL', 'to': '0xf02d804b19b0665690f6b312691b2eb8f80cd3b8', 'input': '0x', 'output': '0x', 'value': '0x3b9aca00'}}}, {'accessList': None, 'blockHash': '0x1839f342b4e0163e747e67705b4a7590067ce338bee8f6ea07a24a62aa5b3831', 'blockNumber': '0x22df23', 'chainId': '0x507', 'creates': None, 'from': '0x5fdcf221e987966b296714953fb6952afc24aefe', 'gas': '0x5208', 'gasPrice': '0x2e90edd000', 'hash': '0xf546335453b6e35ce7e236ee873c96ba3a22602b3acc4f45f5d68b33a76d79ca', 'input': '0x', 'nonce': '0x5092f', 'publicKey': '0x43ad2aac6aa5a7ece7cda8d19e0ddde3ee9aa6211e94695c42c24e21004b5662a3ecaab2f89803ca3a284d2d02d56eb8f68dea86e180a02ab17c41ee254c5a41', 'r': '0xb8ba4494c47bd0b7c2bcc673f6033983732fa4b2a71729f76588b5c0b36657c9', 'raw': '0xf86d8305092f852e90edd000825208945fdcf221e987966b296714953fb6952afc24aefe843b9aca0080820a31a0b8ba4494c47bd0b7c2bcc673f6033983732fa4b2a71729f76588b5c0b36657c9a02f53d53bf4acc343ff1a869e1535de812d1d60ac77fc083be524392b258ed5a4', 's': '0x2f53d53bf4acc343ff1a869e1535de812d1d60ac77fc083be524392b258ed5a4', 'standardV': '0x0', 'to': '0x5fdcf221e987966b296714953fb6952afc24aefe', 'transactionIndex': '0x2', 'type': '0x0', 'v': '0xa31', 'value': '0x3b9aca00', 'receipt_': {'transactionHash': '0xf546335453b6e35ce7e236ee873c96ba3a22602b3acc4f45f5d68b33a76d79ca', 'transactionIndex': '0x2', 'blockHash': '0x1839f342b4e0163e747e67705b4a7590067ce338bee8f6ea07a24a62aa5b3831', 'from': '0x5fdcf221e987966b296714953fb6952afc24aefe', 'to': '0x5fdcf221e987966b296714953fb6952afc24aefe', 'blockNumber': '0x22df23', 'cumulativeGasUsed': '0xf618', 'gasUsed': '0x5208', 'contractAddress': None, 'logs': [], 'logsBloom': '0x00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000', 'status': '0x1', 'effectiveGasPrice': '0x2e90edd000', 'type': '0x0'}, 'debugFrame_': {'result': {'from': '0x5fdcf221e987966b296714953fb6952afc24aefe', 'gas': '0x0', 'gasUsed': '0x0', 'type': 'CALL', 'to': '0x5fdcf221e987966b296714953fb6952afc24aefe', 'input': '0x', 'output': '0x', 'value': '0x3b9aca00'}}}, {'accessList': None, 'blockHash': '0x1839f342b4e0163e747e67705b4a7590067ce338bee8f6ea07a24a62aa5b3831', 'blockNumber': '0x22df23', 'chainId': '0x507', 'creates': None, 'from': '0x3b4c3e2423f1fe18570d5d4e4b4f421b2823f0d1', 'gas': '0x5208', 'gasPrice': '0x77359400', 'hash': '0x4ed713ccd474fc33d2022a802f064cc012e3e37cd22891d4a89c7ba3d776f2db', 'input': '0x', 'nonce': '0x1acd3', 'publicKey': '0x98937a08c30c52f1d7a72d768aee4f0395cb0593eef7cb1b7aedebb3b0cf8cfe7be70d965f7cb8340afbd45013f964b61171faace362d0419b3afce9f246e162', 'r': '0xcfabd3edf059954d0e63aad38b1fbceaadd0032b205aeb71bbed0425df048fbd', 'raw': '0xf8688301acd38477359400825208944b8c667590e6a28497ea4be5facb7e9869a64eae6480820a32a0cfabd3edf059954d0e63aad38b1fbceaadd0032b205aeb71bbed0425df048fbda00305c0f3aa1784a1534f7dcfae5fec12c5d099cfcf64dfa50a13daa80e6cc94d', 's': '0x305c0f3aa1784a1534f7dcfae5fec12c5d099cfcf64dfa50a13daa80e6cc94d', 'standardV': '0x1', 'to': '0x4b8c667590e6a28497ea4be5facb7e9869a64eae', 'transactionIndex': '0x3', 'type': '0x0', 'v': '0xa32', 'value': '0x64', 'receipt_': {'transactionHash': '0x4ed713ccd474fc33d2022a802f064cc012e3e37cd22891d4a89c7ba3d776f2db', 'transactionIndex': '0x3', 'blockHash': '0x1839f342b4e0163e747e67705b4a7590067ce338bee8f6ea07a24a62aa5b3831', 'from': '0x3b4c3e2423f1fe18570d5d4e4b4f421b2823f0d1', 'to': '0x4b8c667590e6a28497ea4be5facb7e9869a64eae', 'blockNumber': '0x22df23', 'cumulativeGasUsed': '0x14820', 'gasUsed': '0x5208', 'contractAddress': None, 'logs': [], 'logsBloom': '0x00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000', 'status': '0x1', 'effectiveGasPrice': '0x77359400', 'type': '0x0'}, 'debugFrame_': {'result': {'from': '0x3b4c3e2423f1fe18570d5d4e4b4f421b2823f0d1', 'gas': '0x0', 'gasUsed': '0x0', 'type': 'CALL', 'to': '0x4b8c667590e6a28497ea4be5facb7e9869a64eae', 'input': '0x', 'output': '0x', 'value': '0x64'}}}, {'accessList': [], 'blockHash': '0x1839f342b4e0163e747e67705b4a7590067ce338bee8f6ea07a24a62aa5b3831', 'blockNumber': '0x22df23', 'chainId': '0x507', 'creates': None, 'from': '0x7161acaef4059dd805df35a4c57d30f2dc31741e', 'gas': '0x6755', 'gasPrice': '0x3b9aca00', 'hash': '0xa5355f86844bb23fe666b10b509543fa377a9e324513eb221e0a2c926a64cae4', 'input': '0x095ea7b300000000000000000000000090a70aae360e5e69c3cb466880f025985810f2c800000000000000000000000000000000000000000000003635c9adc5dea00000', 'maxFeePerGas': '0x3b9aca00', 'maxPriorityFeePerGas': '0x3b9aca00', 'nonce': '0x25b1a', 'publicKey': '0x0e0d9b8fe15cca18122d15fb9a838102da6f8362d7cfc041a9dac6bee2a8ef2a696a18a118f665e0b21fe4e55ee48f5a063b69025164e8602c4fb781e9869619', 'r': '0x73b37d3901fe4f568be776dd217cd2c06c4275540d9e09e73fbf21b05edd24f1', 'raw': '0x02f8b482050783025b1a843b9aca00843b9aca0082675594b8446fd36a5b65e35db34363c42165f97dbff4c680b844095ea7b300000000000000000000000090a70aae360e5e69c3cb466880f025985810f2c800000000000000000000000000000000000000000000003635c9adc5dea00000c080a073b37d3901fe4f568be776dd217cd2c06c4275540d9e09e73fbf21b05edd24f1a03371d91ad1fc82bcb401123bfe39d4404e6ef4a9a136a164c8495f1953b7e265', 's': '0x3371d91ad1fc82bcb401123bfe39d4404e6ef4a9a136a164c8495f1953b7e265', 'standardV': '0x0', 'to': '0xb8446fd36a5b65e35db34363c42165f97dbff4c6', 'transactionIndex': '0x4', 'type': '0x2', 'v': '0x0', 'value': '0x0', 'receipt_': {'transactionHash': '0xa5355f86844bb23fe666b10b509543fa377a9e324513eb221e0a2c926a64cae4', 'transactionIndex': '0x4', 'blockHash': '0x1839f342b4e0163e747e67705b4a7590067ce338bee8f6ea07a24a62aa5b3831', 'from': '0x7161acaef4059dd805df35a4c57d30f2dc31741e', 'to': '0xb8446fd36a5b65e35db34363c42165f97dbff4c6', 'blockNumber': '0x22df23', 'cumulativeGasUsed': '0x1af37', 'gasUsed': '0x6717', 'contractAddress': None, 'logs': [{'address': '0xb8446fd36a5b65e35db34363c42165f97dbff4c6', 'topics': ['0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925', '0x0000000000000000000000007161acaef4059dd805df35a4c57d30f2dc31741e', '0x00000000000000000000000090a70aae360e5e69c3cb466880f025985810f2c8'], 'data': '0x00000000000000000000000000000000000000000000003635c9adc5dea00000', 'blockHash': '0x1839f342b4e0163e747e67705b4a7590067ce338bee8f6ea07a24a62aa5b3831', 'blockNumber': '0x22df23', 'transactionHash': '0xa5355f86844bb23fe666b10b509543fa377a9e324513eb221e0a2c926a64cae4', 'transactionIndex': '0x4', 'logIndex': '0x0', 'transactionLogIndex': '0x0', 'removed': False}], 'logsBloom': '0x00000000000000080000000000000000400000000000000000000000000000000000000000000000000000000000000000000000000000000000000001200000000000000000000000000000000000000000000000000000000000000000000000020000000000000000000000000000000000000002000000000000000000000000000000000000000000000000000000000000002000000000000000000000020000000000000000000000000000000000000000000000000000000000000000000000000000000000000000040000000000000000000001000000000000000010000000000000000000000000000000000000000000000000000020000000', 'status': '0x1', 'effectiveGasPrice': '0x3b9aca00', 'type': '0x2'}, 'debugFrame_': {'result': {'from': '0x7161acaef4059dd805df35a4c57d30f2dc31741e', 'gas': '0x3e', 'gasUsed': '0x128b', 'type': 'CALL', 'to': '0xb8446fd36a5b65e35db34363c42165f97dbff4c6', 'input': '0x095ea7b300000000000000000000000090a70aae360e5e69c3cb466880f025985810f2c800000000000000000000000000000000000000000000003635c9adc5dea00000', 'output': '0x0000000000000000000000000000000000000000000000000000000000000001', 'value': '0x0'}}}, {'accessList': [], 'blockHash': '0x1839f342b4e0163e747e67705b4a7590067ce338bee8f6ea07a24a62aa5b3831', 'blockNumber': '0x22df23', 'chainId': '0x507', 'creates': None, 'from': '0x0da9cb919901bfd8da02f24650ff75eb5bef8f52', 'gas': '0x6755', 'gasPrice': '0x3b9aca00', 'hash': '0xc14791a3a392018fc3438f39cac1d572e8baadd4ed350e0355d1ca874a169e6a', 'input': '0x095ea7b30000000000000000000000007d5b80078480804c126363cf1061c2dc6b9c1b2200000000000000000000000000000000000000000000003635c9adc5dea00000', 'maxFeePerGas': '0x3b9aca00', 'maxPriorityFeePerGas': '0x3b9aca00', 'nonce': '0x14ce8', 'publicKey': '0xb123e2af6a2ea189904d92d911149b5f5f66ac3f0ebf381c1503bb096a13b13bcf060d26821e1a6294e17f39715f261568a935f61567a687f40ce1102c0c2543', 'r': '0xd6b38eb1776397f1db6c5af6cdac53104e0b9b53cacc8560db6382ceeef1a49f', 'raw': '0x02f8b482050783014ce8843b9aca00843b9aca0082675594b8446fd36a5b65e35db34363c42165f97dbff4c680b844095ea7b30000000000000000000000007d5b80078480804c126363cf1061c2dc6b9c1b2200000000000000000000000000000000000000000000003635c9adc5dea00000c001a0d6b38eb1776397f1db6c5af6cdac53104e0b9b53cacc8560db6382ceeef1a49fa033311b2492de7ddc36fcd7a59b62c589d20aa5d5972e89e3379a47f1e395fd0e', 's': '0x33311b2492de7ddc36fcd7a59b62c589d20aa5d5972e89e3379a47f1e395fd0e', 'standardV': '0x1', 'to': '0xb8446fd36a5b65e35db34363c42165f97dbff4c6', 'transactionIndex': '0x5', 'type': '0x2', 'v': '0x1', 'value': '0x0', 'receipt_': {'transactionHash': '0xc14791a3a392018fc3438f39cac1d572e8baadd4ed350e0355d1ca874a169e6a', 'transactionIndex': '0x5', 'blockHash': '0x1839f342b4e0163e747e67705b4a7590067ce338bee8f6ea07a24a62aa5b3831', 'from': '0x0da9cb919901bfd8da02f24650ff75eb5bef8f52', 'to': '0xb8446fd36a5b65e35db34363c42165f97dbff4c6', 'blockNumber': '0x22df23', 'cumulativeGasUsed': '0x2164e', 'gasUsed': '0x6717', 'contractAddress': None, 'logs': [{'address': '0xb8446fd36a5b65e35db34363c42165f97dbff4c6', 'topics': ['0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925', '0x0000000000000000000000000da9cb919901bfd8da02f24650ff75eb5bef8f52', '0x0000000000000000000000007d5b80078480804c126363cf1061c2dc6b9c1b22'], 'data': '0x00000000000000000000000000000000000000000000003635c9adc5dea00000', 'blockHash': '0x1839f342b4e0163e747e67705b4a7590067ce338bee8f6ea07a24a62aa5b3831', 'blockNumber': '0x22df23', 'transactionHash': '0xc14791a3a392018fc3438f39cac1d572e8baadd4ed350e0355d1ca874a169e6a', 'transactionIndex': '0x5', 'logIndex': '0x1', 'transactionLogIndex': '0x0', 'removed': False}], 'logsBloom': '0x00000000000000000000000000000000000040000000000000008000000000000000000000000001000000000080000000000000000000000000000001200000000000000000000000000000000000000000000000000000000000000000000000020000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000020000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000400000000000000001000000000000000010000000000000000000000000000000000000000000000000020000000000', 'status': '0x1', 'effectiveGasPrice': '0x3b9aca00', 'type': '0x2'}, 'debugFrame_': {'result': {'from': '0x0da9cb919901bfd8da02f24650ff75eb5bef8f52', 'gas': '0x3e', 'gasUsed': '0x128b', 'type': 'CALL', 'to': '0xb8446fd36a5b65e35db34363c42165f97dbff4c6', 'input': '0x095ea7b30000000000000000000000007d5b80078480804c126363cf1061c2dc6b9c1b2200000000000000000000000000000000000000000000003635c9adc5dea00000', 'output': '0x0000000000000000000000000000000000000000000000000000000000000001', 'value': '0x0'}}}], 'transactionsRoot': '0xfc1a21484983296aa65659c12a13a0c35ea441cd1b6a6200b9144453c30e28e9', 'uncles': []}
    for tx in block['transactions']:
        if not with_receipts:
            tx.pop('receipt_', None)
        if not with_traces:
            tx.pop('debugFrame_', None)
    return block


def _moonbase_2285348(with_receipts: bool, with_traces: bool) -> Block:
    block: Block = {'author': '0x9ce1fac72010afe03b1c0be421b731ebe8a8022a', 'baseFeePerGas': '0x3b9aca00', 'difficulty': '0x0', 'extraData': '0x', 'gasLimit': '0xe4e1c0', 'gasUsed': '0x79384', 'hash': '0xdadbbbfd8a7f177c466117a6f5635eff51d698d12cec9f5e2b360909621beb43', 'logsBloom': '0x00000000000000080000000000000000400040000000000000008000000000000000000000000001000000000080000000000000000000000000000001200000000000000000000000000000000000000000000000000000000000000000000000020000000000000000000000000000000000000002000000000000000000000000000000000000000000000000000000000000002000000000000000000000020000000000000000000000000000000000000000000000000000000000000000000000000000000000000000040000400000000000000001000000000000000010000000000000000000000000000000000000000000000000020020000000', 'miner': '0x9ce1fac72010afe03b1c0be421b731ebe8a8022a', 'nonce': '0x0000000000000000', 'number': '0x22df24', 'parentHash': '0x1839f342b4e0163e747e67705b4a7590067ce338bee8f6ea07a24a62aa5b3831', 'receiptsRoot': '0x71dea6b4a545a1608da046c9c6056039cdada0b1f839b4be79737cf371fe08e1', 'sha3Uncles': '0x1dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347', 'size': '0xacf', 'stateRoot': '0xbf266b60f968da7e1d7f004ecaf5c742a64cb8ccf2da53d67c1d7c8b9e327453', 'timestamp': '0x62a1ee28', 'totalDifficulty': '0x0', 'transactions': [{'accessList': None, 'blockHash': '0xdadbbbfd8a7f177c466117a6f5635eff51d698d12cec9f5e2b360909621beb43', 'blockNumber': '0x22df24', 'chainId': '0x507', 'creates': '0x492df9cb59aa1042d093ea0a306092ed521ab2f4', 'from': '0xf02d804b19b0665690f6b312691b2eb8f80cd3b8', 'gas': '0x1e208', 'gasPrice': '0xe8d4a51000', 'hash': '0x981f87d83a2adbee21419f0946d85e8d60a33e7add77e6abc5fef3b7d848313c', 'input': '0x608060405260016000806101000a81548160ff02191690831515021790555034801561002a57600080fd5b5060d5806100396000396000f3fe6080604052348015600f57600080fd5b506004361060325760003560e01c80632e64cec1146037578063779541bb146057575b600080fd5b603d6077565b604051808215151515815260200191505060405180910390f35b605d608d565b604051808215151515815260200191505060405180910390f35b60008060009054906101000a900460ff16905090565b6000809054906101000a900460ff168156fea2646970667358221220ff2422b83ea36010f822fd695dbac86360fc54d4d55b0be34768cfa96ad3ba7a64736f6c63430006060033', 'nonce': '0x50847', 'publicKey': '0x6a79dbe1528444f91b06dc487a8ff1160ae2b6bbb2a5546a79c234a34a5a2ebf97694f3cc72f0f1bc51944f0c7a08271765e5f393725762e588b866e5f744cd6', 'r': '0xe8a2af5b6efe776c84fba892c50abf1526b3cb8121b3f330ca0ca034c8b93e60', 'raw': '0xf901668305084785e8d4a510008301e2088080b9010e608060405260016000806101000a81548160ff02191690831515021790555034801561002a57600080fd5b5060d5806100396000396000f3fe6080604052348015600f57600080fd5b506004361060325760003560e01c80632e64cec1146037578063779541bb146057575b600080fd5b603d6077565b604051808215151515815260200191505060405180910390f35b605d608d565b604051808215151515815260200191505060405180910390f35b60008060009054906101000a900460ff16905090565b6000809054906101000a900460ff168156fea2646970667358221220ff2422b83ea36010f822fd695dbac86360fc54d4d55b0be34768cfa96ad3ba7a64736f6c63430006060033820a32a0e8a2af5b6efe776c84fba892c50abf1526b3cb8121b3f330ca0ca034c8b93e60a05b874d6f491352e60a2ea35b79ce845aa55c94f66393ccf7e141859fe77e5a81', 's': '0x5b874d6f491352e60a2ea35b79ce845aa55c94f66393ccf7e141859fe77e5a81', 'standardV': '0x1', 'to': None, 'transactionIndex': '0x6', 'type': '0x0', 'v': '0xa32', 'value': '0x0', 'receipt_': {'transactionHash': '0x981f87d83a2adbee21419f0946d85e8d60a33e7add77e6abc5fef3b7d848313c', 'transactionIndex': '0x6', 'blockHash': '0xdadbbbfd8a7f177c466117a6f5635eff51d698d12cec9f5e2b360909621beb43', 'from': '0xf02d804b19b0665690f6b312691b2eb8f80cd3b8', 'to': None, 'blockNumber': '0x22df24', 'cumulativeGasUsed': '0x3f2c6', 'gasUsed': '0x1dc78', 'contractAddress': '0x492df9cb59aa1042d093ea0a306092ed521ab2f4', 'logs': [], 'logsBloom': '0x00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000', 'status': '0x1', 'effectiveGasPrice': '0xe8d4a51000', 'type': '0x0'}, 'debugFrame_': {'result': {'from': '0xf02d804b19b0665690f6b312691b2eb8f80cd3b8', 'gas': '0xabf8', 'gasUsed': '0x13610', 'type': 'CREATE', 'input': '0x608060405260016000806101000a81548160ff02191690831515021790555034801561002a57600080fd5b5060d5806100396000396000f3fe6080604052348015600f57600080fd5b506004361060325760003560e01c80632e64cec1146037578063779541bb146057575b600080fd5b603d6077565b604051808215151515815260200191505060405180910390f35b605d608d565b604051808215151515815260200191505060405180910390f35b60008060009054906101000a900460ff16905090565b6000809054906101000a900460ff168156fea2646970667358221220ff2422b83ea36010f822fd695dbac86360fc54d4d55b0be34768cfa96ad3ba7a64736f6c63430006060033', 'to': '0x492df9cb59aa1042d093ea0a306092ed521ab2f4', 'output': '0x6080604052348015600f57600080fd5b506004361060325760003560e01c80632e64cec1146037578063779541bb146057575b600080fd5b603d6077565b604051808215151515815260200191505060405180910390f35b605d608d565b604051808215151515815260200191505060405180910390f35b60008060009054906101000a900460ff16905090565b6000809054906101000a900460ff168156fea2646970667358221220ff2422b83ea36010f822fd695dbac86360fc54d4d55b0be34768cfa96ad3ba7a64736f6c63430006060033', 'value': '0x0'}}}, {'accessList': None, 'blockHash': '0xdadbbbfd8a7f177c466117a6f5635eff51d698d12cec9f5e2b360909621beb43', 'blockNumber': '0x22df24', 'chainId': '0x507', 'creates': '0xffc36517134f6f94c785e4d081125b336bb9bb89', 'from': '0x5fdcf221e987966b296714953fb6952afc24aefe', 'gas': '0x1e208', 'gasPrice': '0xe8d4a51000', 'hash': '0x92d992eb84b636094ae37b956410a973b9ff0905dcafa55e9b84addf7300b5f3', 'input': '0x608060405260016000806101000a81548160ff02191690831515021790555034801561002a57600080fd5b5060d5806100396000396000f3fe6080604052348015600f57600080fd5b506004361060325760003560e01c80632e64cec1146037578063779541bb146057575b600080fd5b603d6077565b604051808215151515815260200191505060405180910390f35b605d608d565b604051808215151515815260200191505060405180910390f35b60008060009054906101000a900460ff16905090565b6000809054906101000a900460ff168156fea2646970667358221220ff2422b83ea36010f822fd695dbac86360fc54d4d55b0be34768cfa96ad3ba7a64736f6c63430006060033', 'nonce': '0x50930', 'publicKey': '0x43ad2aac6aa5a7ece7cda8d19e0ddde3ee9aa6211e94695c42c24e21004b5662a3ecaab2f89803ca3a284d2d02d56eb8f68dea86e180a02ab17c41ee254c5a41', 'r': '0xf6c66018f79498603c8490343c1d0ff805eab285400a15f68bd1e060d2815f0c', 'raw': '0xf901668305093085e8d4a510008301e2088080b9010e608060405260016000806101000a81548160ff02191690831515021790555034801561002a57600080fd5b5060d5806100396000396000f3fe6080604052348015600f57600080fd5b506004361060325760003560e01c80632e64cec1146037578063779541bb146057575b600080fd5b603d6077565b604051808215151515815260200191505060405180910390f35b605d608d565b604051808215151515815260200191505060405180910390f35b60008060009054906101000a900460ff16905090565b6000809054906101000a900460ff168156fea2646970667358221220ff2422b83ea36010f822fd695dbac86360fc54d4d55b0be34768cfa96ad3ba7a64736f6c63430006060033820a32a0f6c66018f79498603c8490343c1d0ff805eab285400a15f68bd1e060d2815f0ca042545fce7f0115027f9568a48a31ef78f8e21ce05b76bc4255d20ab860f42c21', 's': '0x42545fce7f0115027f9568a48a31ef78f8e21ce05b76bc4255d20ab860f42c21', 'standardV': '0x1', 'to': None, 'transactionIndex': '0x7', 'type': '0x0', 'v': '0xa32', 'value': '0x0', 'receipt_': {'transactionHash': '0x92d992eb84b636094ae37b956410a973b9ff0905dcafa55e9b84addf7300b5f3', 'transactionIndex': '0x7', 'blockHash': '0xdadbbbfd8a7f177c466117a6f5635eff51d698d12cec9f5e2b360909621beb43', 'from': '0x5fdcf221e987966b296714953fb6952afc24aefe', 'to': None, 'blockNumber': '0x22df24', 'cumulativeGasUsed': '0x5cf3e', 'gasUsed': '0x1dc78', 'contractAddress': '0xffc36517134f6f94c785e4d081125b336bb9bb89', 'logs': [], 'logsBloom': '0x00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000', 'status': '0x1', 'effectiveGasPrice': '0xe8d4a51000', 'type': '0x0'}, 'debugFrame_': {'result': {'from': '0x5fdcf221e987966b296714953fb6952afc24aefe', 'gas': '0xabf8', 'gasUsed': '0x56f4', 'type': 'CREATE', 'input': '0x608060405260016000806101000a81548160ff02191690831515021790555034801561002a57600080fd5b5060d5806100396000396000f3fe6080604052348015600f57600080fd5b506004361060325760003560e01c80632e64cec1146037578063779541bb146057575b600080fd5b603d6077565b604051808215151515815260200191505060405180910390f35b605d608d565b604051808215151515815260200191505060405180910390f35b60008060009054906101000a900460ff16905090565b6000809054906101000a900460ff168156fea2646970667358221220ff2422b83ea36010f822fd695dbac86360fc54d4d55b0be34768cfa96ad3ba7a64736f6c63430006060033', 'to': '0xffc36517134f6f94c785e4d081125b336bb9bb89', 'output': '0x6080604052348015600f57600080fd5b506004361060325760003560e01c80632e64cec1146037578063779541bb146057575b600080fd5b603d6077565b604051808215151515815260200191505060405180910390f35b605d608d565b604051808215151515815260200191505060405180910390f35b60008060009054906101000a900460ff16905090565b6000809054906101000a900460ff168156fea2646970667358221220ff2422b83ea36010f822fd695dbac86360fc54d4d55b0be34768cfa96ad3ba7a64736f6c63430006060033', 'value': '0x0'}}}, {'accessList': None, 'blockHash': '0xdadbbbfd8a7f177c466117a6f5635eff51d698d12cec9f5e2b360909621beb43', 'blockNumber': '0x22df24', 'chainId': '0x507', 'creates': None, 'from': '0x3b4c3e2423f1fe18570d5d4e4b4f421b2823f0d1', 'gas': '0x5208', 'gasPrice': '0x77359400', 'hash': '0x6c149d76e287881f6512f95956caef1bf772bdf367b71dac8336be7d3fdf05ca', 'input': '0x', 'nonce': '0x1acd4', 'publicKey': '0x98937a08c30c52f1d7a72d768aee4f0395cb0593eef7cb1b7aedebb3b0cf8cfe7be70d965f7cb8340afbd45013f964b61171faace362d0419b3afce9f246e162', 'r': '0x39da743d3564db7a7bd8745976d58b62d36849c8c9d9e323857ff79e99506494', 'raw': '0xf8688301acd48477359400825208944b8c667590e6a28497ea4be5facb7e9869a64eae6480820a32a039da743d3564db7a7bd8745976d58b62d36849c8c9d9e323857ff79e99506494a037aa6db1236a5c953d36f4e0ec4b0f43a56b73ee993c677d9f6b71cf54580a61', 's': '0x37aa6db1236a5c953d36f4e0ec4b0f43a56b73ee993c677d9f6b71cf54580a61', 'standardV': '0x1', 'to': '0x4b8c667590e6a28497ea4be5facb7e9869a64eae', 'transactionIndex': '0x8', 'type': '0x0', 'v': '0xa32', 'value': '0x64', 'receipt_': {'transactionHash': '0x6c149d76e287881f6512f95956caef1bf772bdf367b71dac8336be7d3fdf05ca', 'transactionIndex': '0x8', 'blockHash': '0xdadbbbfd8a7f177c466117a6f5635eff51d698d12cec9f5e2b360909621beb43', 'from': '0x3b4c3e2423f1fe18570d5d4e4b4f421b2823f0d1', 'to': '0x4b8c667590e6a28497ea4be5facb7e9869a64eae', 'blockNumber': '0x22df24', 'cumulativeGasUsed': '0x62146', 'gasUsed': '0x5208', 'contractAddress': None, 'logs': [], 'logsBloom': '0x00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000', 'status': '0x1', 'effectiveGasPrice': '0x77359400', 'type': '0x0'}, 'debugFrame_': {'result': {'from': '0x3b4c3e2423f1fe18570d5d4e4b4f421b2823f0d1', 'gas': '0x0', 'gasUsed': '0x0', 'type': 'CALL', 'to': '0x4b8c667590e6a28497ea4be5facb7e9869a64eae', 'input': '0x', 'output': '0x', 'value': '0x64'}}}, {'accessList': None, 'blockHash': '0xdadbbbfd8a7f177c466117a6f5635eff51d698d12cec9f5e2b360909621beb43', 'blockNumber': '0x22df24', 'chainId': '0x507', 'creates': None, 'from': '0x3b4c3e2423f1fe18570d5d4e4b4f421b2823f0d1', 'gas': '0x5208', 'gasPrice': '0x77359400', 'hash': '0x46be5547969a37c6c4bb3eccfe05a8a3f919f8aeb2e6738e29e39e7660077a75', 'input': '0x', 'nonce': '0x1acd5', 'publicKey': '0x98937a08c30c52f1d7a72d768aee4f0395cb0593eef7cb1b7aedebb3b0cf8cfe7be70d965f7cb8340afbd45013f964b61171faace362d0419b3afce9f246e162', 'r': '0x8e2ce8a06412211e4acea4e037b74ae19704e7528809b8fe3c04c42301dc5e32', 'raw': '0xf8688301acd5847735940082520894638c52fde7ad610541fbd4038369f064eef9d3f86480820a31a08e2ce8a06412211e4acea4e037b74ae19704e7528809b8fe3c04c42301dc5e32a01022b96af969cfb331d227e1eb45cc7f9e9ac6b257def0d0769d8cf927c5a7ed', 's': '0x1022b96af969cfb331d227e1eb45cc7f9e9ac6b257def0d0769d8cf927c5a7ed', 'standardV': '0x0', 'to': '0x638c52fde7ad610541fbd4038369f064eef9d3f8', 'transactionIndex': '0x9', 'type': '0x0', 'v': '0xa31', 'value': '0x64', 'receipt_': {'transactionHash': '0x46be5547969a37c6c4bb3eccfe05a8a3f919f8aeb2e6738e29e39e7660077a75', 'transactionIndex': '0x9', 'blockHash': '0xdadbbbfd8a7f177c466117a6f5635eff51d698d12cec9f5e2b360909621beb43', 'from': '0x3b4c3e2423f1fe18570d5d4e4b4f421b2823f0d1', 'to': '0x638c52fde7ad610541fbd4038369f064eef9d3f8', 'blockNumber': '0x22df24', 'cumulativeGasUsed': '0x6734e', 'gasUsed': '0x5208', 'contractAddress': None, 'logs': [], 'logsBloom': '0x00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000', 'status': '0x1', 'effectiveGasPrice': '0x77359400', 'type': '0x0'}, 'debugFrame_': {'result': {'from': '0x3b4c3e2423f1fe18570d5d4e4b4f421b2823f0d1', 'gas': '0x0', 'gasUsed': '0x0', 'type': 'CALL', 'to': '0x638c52fde7ad610541fbd4038369f064eef9d3f8', 'input': '0x', 'output': '0x', 'value': '0x64'}}}, {'accessList': [], 'blockHash': '0xdadbbbfd8a7f177c466117a6f5635eff51d698d12cec9f5e2b360909621beb43', 'blockNumber': '0x22df24', 'chainId': '0x507', 'creates': None, 'from': '0x0da9cb919901bfd8da02f24650ff75eb5bef8f52', 'gas': '0x6755', 'gasPrice': '0x3b9aca00', 'hash': '0xc6215b2420a64deba83b91f78c899356d67b3408b7ec720aad1dd70a45d0ef54', 'input': '0x095ea7b30000000000000000000000007d5b80078480804c126363cf1061c2dc6b9c1b2200000000000000000000000000000000000000000000003635c9adc5dea00000', 'maxFeePerGas': '0x3b9aca00', 'maxPriorityFeePerGas': '0x3b9aca00', 'nonce': '0x14ce9', 'publicKey': '0xb123e2af6a2ea189904d92d911149b5f5f66ac3f0ebf381c1503bb096a13b13bcf060d26821e1a6294e17f39715f261568a935f61567a687f40ce1102c0c2543', 'r': '0x16fa869c3f958ce8c7a9b711e3c5b35fe923d9e240c25ffffd61d2694d1f0420', 'raw': '0x02f8b482050783014ce9843b9aca00843b9aca0082675594b8446fd36a5b65e35db34363c42165f97dbff4c680b844095ea7b30000000000000000000000007d5b80078480804c126363cf1061c2dc6b9c1b2200000000000000000000000000000000000000000000003635c9adc5dea00000c080a016fa869c3f958ce8c7a9b711e3c5b35fe923d9e240c25ffffd61d2694d1f0420a064fc11f8b70de68cf9b19e94b54ed077500dc71129308ca0a42cc858e2812ca9', 's': '0x64fc11f8b70de68cf9b19e94b54ed077500dc71129308ca0a42cc858e2812ca9', 'standardV': '0x0', 'to': '0xb8446fd36a5b65e35db34363c42165f97dbff4c6', 'transactionIndex': '0xa', 'type': '0x2', 'v': '0x0', 'value': '0x0', 'receipt_': {'transactionHash': '0xc6215b2420a64deba83b91f78c899356d67b3408b7ec720aad1dd70a45d0ef54', 'transactionIndex': '0xa', 'blockHash': '0xdadbbbfd8a7f177c466117a6f5635eff51d698d12cec9f5e2b360909621beb43', 'from': '0x0da9cb919901bfd8da02f24650ff75eb5bef8f52', 'to': '0xb8446fd36a5b65e35db34363c42165f97dbff4c6', 'blockNumber': '0x22df24', 'cumulativeGasUsed': '0x6da65', 'gasUsed': '0x6717', 'contractAddress': None, 'logs': [{'address': '0xb8446fd36a5b65e35db34363c42165f97dbff4c6', 'topics': ['0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925', '0x0000000000000000000000000da9cb919901bfd8da02f24650ff75eb5bef8f52', '0x0000000000000000000000007d5b80078480804c126363cf1061c2dc6b9c1b22'], 'data': '0x00000000000000000000000000000000000000000000003635c9adc5dea00000', 'blockHash': '0xdadbbbfd8a7f177c466117a6f5635eff51d698d12cec9f5e2b360909621beb43', 'blockNumber': '0x22df24', 'transactionHash': '0xc6215b2420a64deba83b91f78c899356d67b3408b7ec720aad1dd70a45d0ef54', 'transactionIndex': '0xa', 'logIndex': '0x2', 'transactionLogIndex': '0x0', 'removed': False}], 'logsBloom': '0x00000000000000000000000000000000000040000000000000008000000000000000000000000001000000000080000000000000000000000000000001200000000000000000000000000000000000000000000000000000000000000000000000020000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000020000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000400000000000000001000000000000000010000000000000000000000000000000000000000000000000020000000000', 'status': '0x1', 'effectiveGasPrice': '0x3b9aca00', 'type': '0x2'}, 'debugFrame_': {'result': {'from': '0x0da9cb919901bfd8da02f24650ff75eb5bef8f52', 'gas': '0x3e', 'gasUsed': '0x128b', 'type': 'CALL', 'to': '0xb8446fd36a5b65e35db34363c42165f97dbff4c6', 'input': '0x095ea7b30000000000000000000000007d5b80078480804c126363cf1061c2dc6b9c1b2200000000000000000000000000000000000000000000003635c9adc5dea00000', 'output': '0x0000000000000000000000000000000000000000000000000000000000000001', 'value': '0x0'}}}, {'accessList': [], 'blockHash': '0xdadbbbfd8a7f177c466117a6f5635eff51d698d12cec9f5e2b360909621beb43', 'blockNumber': '0x22df24', 'chainId': '0x507', 'creates': None, 'from': '0x7161acaef4059dd805df35a4c57d30f2dc31741e', 'gas': '0x6755', 'gasPrice': '0x3b9aca00', 'hash': '0x4fe777187f85ebdc4a463eb215549691956dca1e7faf82f186bab46c5d81a04d', 'input': '0x095ea7b300000000000000000000000090a70aae360e5e69c3cb466880f025985810f2c800000000000000000000000000000000000000000000003635c9adc5dea00000', 'maxFeePerGas': '0x3b9aca00', 'maxPriorityFeePerGas': '0x3b9aca00', 'nonce': '0x25b1b', 'publicKey': '0x0e0d9b8fe15cca18122d15fb9a838102da6f8362d7cfc041a9dac6bee2a8ef2a696a18a118f665e0b21fe4e55ee48f5a063b69025164e8602c4fb781e9869619', 'r': '0xe2016730d7640b0c56d492e6395188e374274e11c09a821a5547eb053aac845', 'raw': '0x02f8b482050783025b1b843b9aca00843b9aca0082675594b8446fd36a5b65e35db34363c42165f97dbff4c680b844095ea7b300000000000000000000000090a70aae360e5e69c3cb466880f025985810f2c800000000000000000000000000000000000000000000003635c9adc5dea00000c080a00e2016730d7640b0c56d492e6395188e374274e11c09a821a5547eb053aac845a04f5e067e7cfdb6fea4bd3d9b527d9f6d788eb92bf386a827f71b5d4d672e102f', 's': '0x4f5e067e7cfdb6fea4bd3d9b527d9f6d788eb92bf386a827f71b5d4d672e102f', 'standardV': '0x0', 'to': '0xb8446fd36a5b65e35db34363c42165f97dbff4c6', 'transactionIndex': '0xb', 'type': '0x2', 'v': '0x0', 'value': '0x0', 'receipt_': {'transactionHash': '0x4fe777187f85ebdc4a463eb215549691956dca1e7faf82f186bab46c5d81a04d', 'transactionIndex': '0xb', 'blockHash': '0xdadbbbfd8a7f177c466117a6f5635eff51d698d12cec9f5e2b360909621beb43', 'from': '0x7161acaef4059dd805df35a4c57d30f2dc31741e', 'to': '0xb8446fd36a5b65e35db34363c42165f97dbff4c6', 'blockNumber': '0x22df24', 'cumulativeGasUsed': '0x7417c', 'gasUsed': '0x6717', 'contractAddress': None, 'logs': [{'address': '0xb8446fd36a5b65e35db34363c42165f97dbff4c6', 'topics': ['0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925', '0x0000000000000000000000007161acaef4059dd805df35a4c57d30f2dc31741e', '0x00000000000000000000000090a70aae360e5e69c3cb466880f025985810f2c8'], 'data': '0x00000000000000000000000000000000000000000000003635c9adc5dea00000', 'blockHash': '0xdadbbbfd8a7f177c466117a6f5635eff51d698d12cec9f5e2b360909621beb43', 'blockNumber': '0x22df24', 'transactionHash': '0x4fe777187f85ebdc4a463eb215549691956dca1e7faf82f186bab46c5d81a04d', 'transactionIndex': '0xb', 'logIndex': '0x3', 'transactionLogIndex': '0x0', 'removed': False}], 'logsBloom': '0x00000000000000080000000000000000400000000000000000000000000000000000000000000000000000000000000000000000000000000000000001200000000000000000000000000000000000000000000000000000000000000000000000020000000000000000000000000000000000000002000000000000000000000000000000000000000000000000000000000000002000000000000000000000020000000000000000000000000000000000000000000000000000000000000000000000000000000000000000040000000000000000000001000000000000000010000000000000000000000000000000000000000000000000000020000000', 'status': '0x1', 'effectiveGasPrice': '0x3b9aca00', 'type': '0x2'}, 'debugFrame_': {'result': {'from': '0x7161acaef4059dd805df35a4c57d30f2dc31741e', 'gas': '0x3e', 'gasUsed': '0x128b', 'type': 'CALL', 'to': '0xb8446fd36a5b65e35db34363c42165f97dbff4c6', 'input': '0x095ea7b300000000000000000000000090a70aae360e5e69c3cb466880f025985810f2c800000000000000000000000000000000000000000000003635c9adc5dea00000', 'output': '0x0000000000000000000000000000000000000000000000000000000000000001', 'value': '0x0'}}}, {'accessList': None, 'blockHash': '0xdadbbbfd8a7f177c466117a6f5635eff51d698d12cec9f5e2b360909621beb43', 'blockNumber': '0x22df24', 'chainId': '0x507', 'creates': None, 'from': '0x1e2682b77b43109889366c2d0e591f44ab7df419', 'gas': '0x5208', 'gasPrice': '0x3b9aca00', 'hash': '0x6f434385f688ed43c2a2cd59e65b0c8ec91cad7e1913e7a5422dd932081960de', 'input': '0x', 'nonce': '0x207', 'publicKey': '0x35fdba067580f109ec37a2f81574fb3ab9532381acb30b7a62eb7e2a10fae031c64dddcba61106b65b057b0eb79b05c65d6e3884debe202044802d6079eddefe', 'r': '0xda6829d43c45ffec9fe975de7cef8eba8e8734d91253ba4753ca509d5916f771', 'raw': '0xf86e820207843b9aca0082520894a429f7bea7e847ad5e9eeab9eefe966a6fa1eb89872386f26fc1000080820a31a0da6829d43c45ffec9fe975de7cef8eba8e8734d91253ba4753ca509d5916f771a0718937f056b1f563a32d02475239a1d6b2081e2326af7bf6b8008bca64d2c9e1', 's': '0x718937f056b1f563a32d02475239a1d6b2081e2326af7bf6b8008bca64d2c9e1', 'standardV': '0x0', 'to': '0xa429f7bea7e847ad5e9eeab9eefe966a6fa1eb89', 'transactionIndex': '0xc', 'type': '0x0', 'v': '0xa31', 'value': '0x2386f26fc10000', 'receipt_': {'transactionHash': '0x6f434385f688ed43c2a2cd59e65b0c8ec91cad7e1913e7a5422dd932081960de', 'transactionIndex': '0xc', 'blockHash': '0xdadbbbfd8a7f177c466117a6f5635eff51d698d12cec9f5e2b360909621beb43', 'from': '0x1e2682b77b43109889366c2d0e591f44ab7df419', 'to': '0xa429f7bea7e847ad5e9eeab9eefe966a6fa1eb89', 'blockNumber': '0x22df24', 'cumulativeGasUsed': '0x79384', 'gasUsed': '0x5208', 'contractAddress': None, 'logs': [], 'logsBloom': '0x00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000', 'status': '0x1', 'effectiveGasPrice': '0x3b9aca00', 'type': '0x0'}, 'debugFrame_': {'result': {'from': '0x1e2682b77b43109889366c2d0e591f44ab7df419', 'gas': '0x0', 'gasUsed': '0x0', 'type': 'CALL', 'to': '0xa429f7bea7e847ad5e9eeab9eefe966a6fa1eb89', 'input': '0x', 'output': '0x', 'value': '0x2386f26fc10000'}}}], 'transactionsRoot': '0xe211902c59519f49f68591aa0b34abf4512db044b2e6ae27e39c45d2e34aa349', 'uncles': []}
    for tx in block['transactions']:
        if not with_receipts:
            tx.pop('receipt_', None)
        if not with_traces:
            tx.pop('debugFrame_', None)
    return block
