import asyncio
import logging
from collections import defaultdict
from collections.abc import AsyncIterator, Generator
from typing import cast

from sqa.eth.ingest.ingest import _run_subtasks
from sqa.starknet.writer.model import (
    Block,
    BlockStateUpdate,
    Call,
    Event,
    EventPage,
    Trace,
    TraceRoot,
    WriterBlock,
    WriterBlockStateUpdate,
    WriterCall,
    WriterEvent,
    WriterStorageDiffItem,
    WriterTransaction,
)
from sqa.util.rpc.client import RpcClient

LOG = logging.getLogger(__name__)

LOGS_CHUNK_SIZE = 200
STARKNET_FINALITY = 10  # NOTE: https://book.starknet.io/ch03-01-01-transactions-lifecycle.html


class IngestStarknet:
    def __init__(
        self,
        rpc: RpcClient,
        from_block: int = 0,
        to_block: int | None = None,
        with_receipts: bool = False,
        with_traces: bool = False,
        with_statediffs: bool = False,
        validate_tx_root: bool = False,
    ) -> None:
        self._rpc = rpc
        self._finality_confirmation = STARKNET_FINALITY
        self._with_receipts = with_receipts
        self._with_traces = with_traces
        self._with_statediffs = with_statediffs
        self._validate_tx_root = validate_tx_root
        self._height = from_block - 1
        self._end = to_block
        self._chain_height = 0
        self._strides: list[asyncio.Task] = []
        self._stride_size = 20
        self._closed = False
        self._running = False

    async def loop(self) -> AsyncIterator[list[WriterBlock]]:
        if self._running:
            raise RuntimeError('IngestStarknet loop is already running')
        self._running = True
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

    def close(self) -> None:
        self._closed = True
        self._rpc.close()

    def _is_finished(self) -> bool:
        if self._end is None:
            return False
        return self._height >= self._end

    async def _wait_chain(self) -> None:
        stride_size = self._stride_size

        if self._end is not None:
            stride_size = min(stride_size, self._end - self._height)

        if self._dist() < stride_size:
            self._chain_height = await self._get_chain_height()

        while self._dist() <= 0:
            await asyncio.sleep(2)
            self._chain_height = await self._get_chain_height()

    def _schedule_strides(self) -> None:
        while len(self._strides) < max(1, min(10, self._rpc.get_total_capacity())) \
                and not self._is_finished() \
                and self._dist() > 0:
            from_block = self._height + 1
            stride_size = min(self._stride_size, self._dist())
            if self._end is not None:
                stride_size = min(stride_size, self._end - self._height)
            to_block = self._height + stride_size
            task = asyncio.create_task(self._fetch_starknet_stride(from_block, to_block))
            self._strides.append(task)
            self._height = to_block

    def _dist(self) -> int:
        return self._chain_height - self._height

    async def _fetch_starknet_stride(self, from_block: int, to_block: int) -> list[WriterBlock]:
        extra = {'first_block': from_block, 'last_block': to_block}

        LOG.debug('fetching new stride', extra=extra)

        blocks = await self._fetch_starknet_blocks(from_block, to_block)

        await _run_subtasks(self._starknet_stride_subtasks(blocks))

        # NOTE: This code moved here to separate retrieving raw data as it is in node from preparing data to comply with archive format
        strides: list[WriterBlock] = self.make_writer_ready_blocks(blocks)
        LOG.debug('stride is ready', extra=extra)
        return strides

    def _starknet_stride_subtasks(self, blocks: list[Block]) -> Generator[None]:
        if self._with_receipts:
            yield self._fetch_starknet_receipts(blocks)
        else:
            yield self._fetch_starknet_logs(blocks)

        if self._with_traces:
            yield self._fetch_starknet_traces(blocks)

        if self._with_statediffs:
            yield self._fetch_starknet_state_updates(blocks)

    async def _fetch_starknet_logs(self, blocks: list[Block]) -> None:
        priority = blocks[0]['block_number']

        first_page: EventPage = await self._rpc.call(
            'starknet_getEvents',
            [{
                'from_block': {'block_number': blocks[0]['block_number']},
                'to_block': {'block_number': blocks[-1]['block_number']},
                'chunk_size': LOGS_CHUNK_SIZE,
            }],
            priority=priority,
        )
        pages = [first_page]

        # NOTE: get rest of the pages
        while pages[-1].get('continuation_token'):
            pages.append(await self._rpc.call(
                'starknet_getEvents',
                [{
                    'from_block': {'block_number': blocks[0]['block_number']},
                    'to_block': {'block_number': blocks[-1]['block_number']},
                    'chunk_size': LOGS_CHUNK_SIZE,
                    'continuation_token': pages[-1].get('continuation_token'),
                }],
                priority=priority,
            ))

        logs_by_hash: dict[str, list[Event]] = {}
        for page in pages:
            for event in page['events']:
                block_logs = logs_by_hash.setdefault(event['block_hash'], [])
                block_logs.append(event)

        for block in blocks:
            block['events'] = logs_by_hash.get(block['block_hash'], [])

    async def _fetch_starknet_receipts(self, blocks: list[Block]) -> None:
        raise NotImplementedError('receipts for starknet not implemented') # TODO: https://docs.alchemy.com/reference/starknet-gettransactionreceipt for block for tx

    async def _fetch_starknet_traces(self, blocks: list[Block]) -> None:
        for block in blocks:
            num = block['block_number']

            traces: list[Trace] = await self._rpc.call(
                'starknet_traceBlockTransactions',
                params=[{'block_number': num}],
                priority=num,
            )
            block['traces'] = traces

    async def _fetch_starknet_state_updates(self, blocks: list[Block]) -> None:
        for block in blocks:
            num = block['block_number']

            state_update: BlockStateUpdate = await self._rpc.call(
                'starknet_getStateUpdate',
                params=[{'block_number': num}],
                priority=num,
            )

            block['state_update'] = state_update

    async def _detect_special_chains(self) -> None:
        self._is_starknet = True

    async def _fetch_starknet_blocks(self, from_block: int, to_block: int) -> list[Block]:
        return await self._rpc.batch_call(
            [
                ('starknet_getBlockWithTxs', [{'block_number': i}])
                for i in range(from_block, to_block + 1)
            ],
            priority=from_block,
        )

    async def _get_chain_height(self) -> int:
        height = await self._rpc.call('starknet_blockNumber')
        return max(height - self._finality_confirmation, 0)

    @staticmethod
    def make_writer_ready_blocks(blocks: list[Block]) -> list[WriterBlock]:  # noqa: C901
        # NOTE: care for efficiency function modify existing list as well as returning it with different typing
        stride: list[WriterBlock] = cast(list[WriterBlock], blocks)  # cast ahead for less mypy problems
        # NOTE: This function transform exact RPC node objects to Writer object with all extra fields for writing to table
        transaction_hash_to_index = {}
        for block in stride:
            block['number'] = block['block_number']
            block['hash'] = block['block_hash']

            block['writer_txs'] = cast(list[WriterTransaction], block['transactions'])
            for transaction_index, tx in enumerate(block['writer_txs']):
                tx['transaction_index'] = transaction_index
                tx['block_number'] = block['block_number']

                transaction_hash_to_index[tx['transaction_hash']] = tx['transaction_index']

        # could be done with one dict, but i thought its nicer with two
        for block in stride:
            event_index: dict[str, int] = defaultdict(lambda: 0)
            block['writer_events'] = cast(list[WriterEvent], block['events'])
            for event in block['writer_events']:
                event['transaction_index'] = transaction_hash_to_index[event['transaction_hash']]
                event['event_index'] = event_index[event['transaction_hash']]
                event_index[event['transaction_hash']] += 1

        # NOTE: Generate WriterCall list and WriterMessage list from trace and call tree
        for block in stride:
            if 'traces' not in block:
                continue
            block_number = block['block_number']

            block['writer_call_traces'] = []

            for trace in block['traces']:
                transaction_index = transaction_hash_to_index[trace['transaction_hash']]
                trace_root = trace['trace_root']
                block['writer_call_traces'] += unwrap_trace_calls(trace_root, block_number, transaction_index)

        for block in stride:
            if 'state_update' not in block:
                continue
            block['writer_state_update'] = cast(list[WriterBlockStateUpdate], block['state_update'])
            block['writer_state_update']['block_number'] = block['block_number']

        for block in stride:
            if 'state_update' not in block:
                continue
            block['writer_storage_diffs'] = cast(list[WriterStorageDiffItem],
                                                 block['state_update']['state_diff']['storage_diffs'])
            for storage_diff in block['writer_storage_diffs']:
                storage_diff['block_number'] = block['block_number']

        return stride


def unwrap_trace_calls(trace_root: TraceRoot, block_number: int, transaction_index: int) -> list[WriterCall]:
    writer_calls = []
    call_index = 0

    prev_invocation_event_index = 0
    cur_invocation_event_index = 0

    trace_type = trace_root['type']

    def traverse_call_tree(trace_call: Call, invocation_type: str, trace_address: list[int]) -> None:
        nonlocal call_index
        nonlocal cur_invocation_event_index

        trace_call['block_number'] = block_number
        trace_call['transaction_index'] = transaction_index
        trace_call['trace_type'] = trace_type
        trace_call['invocation_type'] = invocation_type

        current_trace_address = [*trace_address, call_index]
        trace_call['trace_address'] = current_trace_address

        # NOTE: convert order to event_index
        for event in trace_call.get('events', []):
            event['order'] += prev_invocation_event_index
            cur_invocation_event_index = max(cur_invocation_event_index, prev_invocation_event_index + event['order'] + 1)

        writer_calls.append(cast(WriterCall, trace_call))
        call_index += 1

        for child_call in trace_call.get('calls', []):
            traverse_call_tree(child_call, invocation_type, current_trace_address)

    if 'execute_invocation' in trace_root:
        traverse_call_tree(trace_root['execute_invocation'], 'execute', [])
    prev_invocation_event_index = cur_invocation_event_index
    if 'constructor_invocation' in trace_root:
        traverse_call_tree(trace_root['constructor_invocation'], 'constructor', [])
    prev_invocation_event_index = cur_invocation_event_index
    if 'validate_invocation' in trace_root:
        traverse_call_tree(trace_root['validate_invocation'], 'validate', [])
    prev_invocation_event_index = cur_invocation_event_index
    if 'fee_transfer_invocation' in trace_root:
        traverse_call_tree(trace_root['fee_transfer_invocation'], 'fee_transfer', [])

    return writer_calls
