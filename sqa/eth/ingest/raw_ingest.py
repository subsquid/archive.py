from typing import AsyncIterator, Optional
import asyncio
import math
import logging
import json
import gzip

from sqa.eth.ingest.model import Block
from sqa.eth.ingest.util import qty2int
from sqa.layout import get_chunks, DataChunk
from sqa.fs import Fs


LOG = logging.getLogger(__name__)


class RawIngest:
    def __init__(
        self,
        fs: Fs,
        from_block: int = 0,
        to_block: Optional[int] = None,
    ):
        self._fs = fs
        self._height = from_block
        self._from_block = from_block
        self._to_block = to_block or math.inf
        self._queue = asyncio.Queue(maxsize=3)

    async def loop(self) -> AsyncIterator[list[Block]]:
        asyncio.create_task(self._schedule_futures())

        while True:
            future = await self._queue.get()
            if future is None:
                return

            blocks = await future
            self._queue.task_done()
            yield blocks

    async def _schedule_futures(self):
        loop = asyncio.get_event_loop()

        async for chunk in self._stream_chunks():
            future = loop.run_in_executor(None, self._load_chunk, chunk)
            await self._queue.put(future)

        await self._queue.put(None)

    async def _stream_chunks(self) -> AsyncIterator[DataChunk]:
        while self._height < self._to_block:
            pos = self._height
            from_block = self._height + 1

            for chunk in get_chunks(self._fs, first_block=from_block, last_block=self._to_block):
                yield chunk
                self._height = chunk.last_block

            if pos == self._height:
                LOG.info('no chunks were found. waiting 5 min for a new try')
                await asyncio.sleep(5 * 60)

    def _load_chunk(self, chunk: DataChunk) -> list[Block]:
        LOG.debug(f'loading chunk {chunk.path()}')
        with self._fs.open(f'{chunk.path()}/blocks.jsonl.gz', 'rb') as f, gzip.open(f) as lines:
            blocks = []
            for line in lines:
                block: Block = json.loads(line)
                height = qty2int(block['number'])
                if self._from_block <= height <= self._to_block:
                    blocks.append(block)
            return blocks
