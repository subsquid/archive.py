import logging
import math
import time
from functools import cached_property
from typing import Iterable, Protocol

from sqa.fs import create_fs, Fs
from sqa.layout import ChunkWriter
from sqa.util.counters import Progress


LOG = logging.getLogger(__name__)


Block = dict


class Sink(Protocol):
    def write(self, block: Block) -> None:
        pass

    def flush(self, fs: Fs) -> None:
        pass

    def buffered_bytes(self) -> int:
        pass


class ArchiveWriteOptions:
    def __init__(self,
                 dest: str,
                 first_block: int = 0,
                 last_block: int | None = None,
                 chunk_size: int = 1024,
                 s3_endpoint: str | None = None
                 ):
        self.dest = dest
        self.first_block = first_block
        self.last_block = last_block if last_block is not None else math.inf
        self.chunk_size = chunk_size
        self.s3_endpoint = s3_endpoint

    def get_block_height(self, block: Block) -> int:
        raise NotImplementedError()

    def get_block_hash(self, block: Block) -> str:
        raise NotImplementedError()

    def get_block_parent_hash(self, block: Block) -> str:
        raise NotImplementedError()

    def chunk_check(self, filelist: list[str]) -> bool:
        for f in filelist:
            if f.startswith('blocks.'):
                return True
        return False


class ArchiveWriter:
    def __init__(self, options: ArchiveWriteOptions):
        self.options = options
        self.fs = create_fs(options.dest)
        self._writing = False

    @cached_property
    def _chunk_writer(self) -> ChunkWriter:
        return ChunkWriter(
            self.fs,
            self.options.chunk_check,
            first_block=self.options.first_block,
            last_block=self.options.last_block
        )

    def get_next_block(self) -> int:
        return self._chunk_writer.next_block

    @cached_property
    def _progress(self) -> Progress:
        progress = Progress(window_size=10, window_granularity_seconds=1)
        progress.set_current_value(self.get_next_block())
        return progress

    def _report(self) -> None:
        LOG.info(
            f'last block: {self._progress.get_current_value()}, '
            f'progress: {round(self._progress.speed())} blocks/sec'
        )

    def _get_hash(self, block: Block) -> str:
        return _short_hash(self.options.get_block_hash(block))

    def _get_parent_hash(self, block: Block) -> str:
        return _short_hash(self.options.get_block_parent_hash(block))

    def _get_height(self, block: Block) -> int:
        return self.options.get_block_height(block)

    def write(self, sink: Sink, strides: Iterable[list[Block]]) -> bool:
        assert not self._writing
        self._writing = True

        write_range = self.get_next_block(), self.options.last_block
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
            chunk_fs = self.fs.cd(chunk.path())
            LOG.info(f'writing {chunk_fs.abs()}')
            with chunk_fs.transact('.') as fs:
                sink.flush(fs)

        for stride in strides:
            first_block = self._get_height(stride[0])
            last_block = self._get_height(stride[-1])
            LOG.debug('got stride', extra={'first_block': first_block, 'last_block': last_block})

            assert write_range[0] <= first_block <= last_block <= write_range[1]

            for block in stride:
                # validate chain continuity
                block_hash = self._get_hash(block)
                block_parent_hash = self._get_parent_hash(block)
                if last_hash and last_hash != block_parent_hash:
                    raise Exception(
                        f'broken chain: block {self._get_height(block)}#{block_hash} '
                        f'is not a direct child of {self._get_height(block) - 1}#{last_hash}'
                    )
                last_hash = block_hash

                # write block
                sink.write(block)

            if chunk_first_block is None:
                chunk_first_block = first_block

            if sink.buffered_bytes() > self.options.chunk_size * 1024 * 1024:
                flush()

            current_time = time.time()
            self._progress.set_current_value(last_block, current_time)
            if current_time - last_report > 5:
                self._report()
                last_report = current_time

        if sink.buffered_bytes() > 0 and last_block == write_range[1]:
            flush()

        if self._progress.has_news():
            self._report()

        return last_block == write_range[1]


def _short_hash(value: str) -> str:
    """Takes first 4 bytes from the hash"""
    assert value.startswith('0x')
    return value[2:10]
