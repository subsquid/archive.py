import logging
import math
import os
import time
from functools import cached_property
from typing import Iterable

from sqa.fs import create_fs, LocalFs
from sqa.layout import ChunkWriter, DataChunk
from sqa.util.counters import Progress


LOG = logging.getLogger(__name__)


Block = dict


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
        self.fs = create_fs(options.dest, s3_endpoint=options.s3_endpoint)
        self._chunk_first_block = None
        self._chunk_last_block = None
        self._chunk_last_hash = None
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

    def get_last_block(self) -> int:
        return self._chunk_writer.last_block

    def get_last_hash(self) -> str | None:
        return self._chunk_writer.last_hash

    def get_last_seen_block(self) -> int:
        if self._chunk_last_block is None:
            return self.get_next_block()
        else:
            return self._chunk_last_block

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

    def write_stream(self, strides: Iterable[list[Block]]) -> Iterable[list[Block]]:
        assert not self._writing
        self._writing = True

        last_report = 0
        last_hash = self.get_last_hash()

        for stride in strides:
            first_block = self._get_height(stride[0])
            last_block = self._get_height(stride[-1])
            LOG.debug('got stride', extra={'first_block': first_block, 'last_block': last_block})

            # validate chain continuity
            for block in stride:
                block_hash = self._get_hash(block)
                block_parent_hash = self._get_parent_hash(block)
                if last_hash and last_hash != block_parent_hash:
                    raise Exception(
                        f'broken chain: block {self._get_height(block)}#{block_hash} '
                        f'is not a direct child of {self._get_height(block) - 1}#{last_hash}'
                    )
                last_hash = block_hash

            self._chunk_last_block = last_block
            self._chunk_last_hash = last_hash
            if self._chunk_first_block is None:
                self._chunk_first_block = first_block

            yield stride

            current_time = time.time()
            self._progress.set_current_value(last_block, current_time)
            if current_time - last_report > 5:
                self._report()
                last_report = current_time

        if self._progress.has_news():
            self._report()

    def next_chunk(self) -> DataChunk:
        assert self._chunk_first_block is not None
        assert self._chunk_last_block is not None
        assert self._chunk_last_hash is not None

        chunk = self._chunk_writer.next_chunk(
            self._chunk_first_block,
            self._chunk_last_block,
            self._chunk_last_hash
        )

        self._chunk_first_block = None
        self._chunk_last_block = None
        self._chunk_last_hash = None
        return chunk

    def write_json_lines(self, strides: Iterable[list[Block]]) -> None:
        import pyarrow
        import tempfile
        import json

        chunk_size = self.options.chunk_size
        stride_stream = self.write_stream(strides)

        while True:
            written = 0
            tmp = tempfile.NamedTemporaryFile(delete=False)
            try:
                with tmp, pyarrow.CompressedOutputStream(tmp, 'gzip') as out:
                    for stride in stride_stream:
                        for block in stride:
                            line = json.dumps(block).encode('utf-8')
                            out.write(line)
                            out.write(b'\n')
                            written += len(line) + 1

                        if written > chunk_size * 1024 * 1024:
                            break

                if written > 0:
                    chunk = self.next_chunk()

                    LOG.debug(f'time to write a chunk {chunk.path()}')
                    dest = f'{chunk.path()}/blocks.jsonl.gz'

                    if isinstance(self.fs, LocalFs):
                        loc = self.fs.abs(dest)
                        os.makedirs(os.path.dirname(loc), exist_ok=True)
                        os.rename(tmp.name, loc)
                    else:
                        self._upload_temp_file(tmp.name, dest)
                else:
                    return
            except:
                os.remove(tmp.name)
                raise

    def _upload_temp_file(self, tmp: str, dest: str):
        try:
            self.fs.upload(tmp, dest)
        finally:
            os.remove(tmp)


def _short_hash(value: str) -> str:
    """Takes first 4 bytes from the hash"""
    assert value.startswith('0x')
    return value[2:10]
