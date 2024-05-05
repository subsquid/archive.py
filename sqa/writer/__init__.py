import logging
import math
import time
from functools import cached_property
from typing import Iterable, Protocol, Any

from sqa.fs import create_fs, Fs
from sqa.layout import ChunkWriter
from sqa.util.counters import Progress


LOG = logging.getLogger(__name__)


Block = dict


class Writer(Protocol):
    def buffered_bytes(self) -> int:
        raise NotImplementedError()

    def push(self, block: Any) -> None:
        pass

    def flush(self, fs: Fs) -> None:
        pass

    def end(self) -> None:
        pass

    def get_block_height(self, block: Block) -> int:
        pass

    def get_block_hash(self, block: Block) -> str:
        pass

    def get_block_parent_hash(self, block: Block) -> str:
        pass

    def chunk_check(self, filelist: list[str]) -> bool:
        for f in filelist:
            if f.startswith('blocks.'):
                return True
        return False


class Sink:
    def __init__(
            self,
            writer: Writer,
            dest: str,
            first_block: int = 0,
            last_block: int | None = None,
            chunk_size: int = 1024,
            top_dir_size: int = 500,
            validate_chain_continuity: bool = True
    ):
        self._writer = writer
        self._fs = create_fs(dest)
        self._first_block = first_block
        self._last_block = last_block if last_block is not None else math.inf
        self._chunk_size = chunk_size
        self._top_dir_size = top_dir_size
        self._writing = False
        self._last_seen_block = -1
        self._last_flushed_block = 1
        self._validate_chain_continuity = validate_chain_continuity

    @cached_property
    def _chunk_writer(self) -> ChunkWriter:
        return ChunkWriter(
            self._fs,
            self._writer.chunk_check,
            first_block=self._first_block,
            last_block=self._last_block,
            top_dir_size=self._top_dir_size
        )

    def get_next_block(self) -> int:
        return self._chunk_writer.next_block

    @cached_property
    def _progress(self) -> Progress:
        progress = Progress(window_size=10, window_granularity_seconds=1)
        progress.set_current_value(self.get_next_block())
        return progress

    def get_progress_speed(self) -> float:
        return self._progress.speed()

    def get_last_seen_block(self) -> int:
        return self._last_seen_block

    def get_last_flushed_block(self) -> int:
        return self._last_flushed_block

    def _report(self) -> None:
        LOG.info(
            f'last block: {self._progress.get_current_value()}, '
            f'progress: {round(self._progress.speed())} blocks/sec'
        )

    def _get_hash(self, block: Block) -> str:
        return _short_hash(self._writer.get_block_hash(block))

    def _get_parent_hash(self, block: Block) -> str:
        return _short_hash(self._writer.get_block_parent_hash(block))

    def _get_height(self, block: Block) -> int:
        return self._writer.get_block_height(block)

    def write(self, strides: Iterable[list[Block]]) -> None:
        assert not self._writing
        self._writing = True

        write_range = self.get_next_block(), self._last_block
        last_report = 0
        last_hash = self._chunk_writer.last_hash

        chunk_first_block = None
        last_block = None

        def flush():
            nonlocal chunk_first_block
            chunk = self._chunk_writer.next_chunk(
                chunk_first_block,
                last_block,
                last_hash
            )
            chunk_first_block = None
            chunk_fs = self._fs.cd(chunk.path())
            LOG.info(f'writing {chunk_fs.abs()}')
            self._writer.flush(chunk_fs)
            self._last_flushed_block = last_block

        for stride in strides:
            first_block = self._get_height(stride[0])
            last_block = self._get_height(stride[-1])
            LOG.debug('got stride', extra={'first_block': first_block, 'last_block': last_block})

            assert write_range[0] <= first_block <= last_block <= write_range[1]

            for block in stride:
                if self._validate_chain_continuity:
                    block_hash = self._get_hash(block)
                    block_parent_hash = self._get_parent_hash(block)
                    if last_hash and last_hash != block_parent_hash:
                        raise Exception(
                            f'broken chain: block {self._get_height(block)}#{block_hash} '
                            f'is not a direct child of {self._get_height(block) - 1}#{last_hash}'
                        )
                    last_hash = block_hash
                else:
                    last_hash = self._get_hash(block)

                # write block
                self._writer.push(block)

            if chunk_first_block is None:
                chunk_first_block = first_block

            self._last_seen_block = last_block

            if self._writer.buffered_bytes() > self._chunk_size * 1024 * 1024:
                flush()

            current_time = time.time()
            self._progress.set_current_value(last_block, current_time)
            if current_time - last_report > 5:
                self._report()
                last_report = current_time

        if self._writer.buffered_bytes() > 0 and last_block == write_range[1]:
            flush()

        self._writer.end()

        if self._progress.has_news():
            self._report()


def _short_hash(value: str) -> str:
    if value.startswith('0x'):
        return value[2:10]
    else:
        return value[0:5]
