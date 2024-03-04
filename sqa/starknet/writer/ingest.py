import asyncio
import logging
from typing import AsyncIterator, cast
from sqa.starknet.writer.model import EventPage, Block, Event
from sqa.eth.ingest.ingest import Ingest, _run_subtasks, _txs_with_missing_status

LOG = logging.getLogger(__name__)

LOGS_CHUNK_SIZE = 200  # TODO: think about

class IngestStarknet(Ingest):

    def _schedule_strides(self):
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

    async def _fetch_starknet_stride(self, from_block: int, to_block: int) -> list[Block]:
        extra = {'first_block': from_block, 'last_block': to_block}

        LOG.debug('fetching new stride', extra=extra)

        blocks = await self._fetch_starknet_blocks(from_block, to_block)

        await _run_subtasks(self._starknet_stride_subtasks(blocks))

        # TODO: if self._with_traces and not self._with_receipts:_txs_with_missing_status

        LOG.debug('stride is ready', extra=extra)
        return blocks

    def _starknet_stride_subtasks(self, blocks: list[Block]):
        if self._with_receipts:
            yield self._fetch_starknet_receipts(blocks)
        else:
            yield self._fetch_starknet_logs(blocks)

        # TODO: _fetch_traces

    async def _fetch_starknet_logs(self, blocks: list[Block]) -> None:
        priority = blocks[0]['number']

        first_page: EventPage = await self._rpc.call(
            'starknet_getEvents',
            [{
                'from_block': {'block_number': blocks[0]['number']},
                'to_block': {'block_number': blocks[-1]['number']},
                'chunk_size': LOGS_CHUNK_SIZE,
            }],
            priority=priority
        )
        pages = [first_page]
        
        # NOTE: get rest of the pages
        while pages[-1].get('continuation_token'):
            pages.append(await self._rpc.call(
                'starknet_getEvents',
                [{
                    'from_block': {'block_number': blocks[0]['number']},
                    'to_block': {'block_number': blocks[-1]['number']},
                    'chunk_size': LOGS_CHUNK_SIZE,
                    'continuation_token': pages[-1].get('continuation_token')
                }],
                priority=priority
            ))

        logs_by_hash: dict[str, list[Event]] = {}
        for page in pages:
            for event in page['events']:
                block_logs = logs_by_hash.setdefault(event['block_hash'], [])
                block_logs.append(event)
        
        for block in blocks:
            block['logs_'] = logs_by_hash.get(block['block_hash'], [])

    async def _fetch_starknet_receipts(self, blocks: list[Block]) -> None:
        raise NotImplementedError('receipts for starknet not implemented') # TODO: https://docs.alchemy.com/reference/starknet-gettransactionreceipt for block for tx

    async def _detect_special_chains(self) -> None:
        self._is_starknet = True

    async def _fetch_starknet_blocks(self, from_block: int, to_block: int) -> list[Block]:
        blocks = await self._rpc.batch_call(
            [
                ('starknet_getBlockWithTxs', [{'block_number': i}])
                for i in range(from_block, to_block + 1)
            ],
            priority=from_block
        )

        for block in blocks:
            block_number = block.pop('block_number')
            block['number'] = block_number
            for tx in block['transactions']:
                tx['block_number'] = block_number
        
        # TODO: validate tx root?

        return blocks

    async def _get_chain_height(self) -> int:
        height = (await self._rpc.call('starknet_blockNumber'))
        return max(height - self._finality_confirmation, 0)
