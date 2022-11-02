import bisect
import math
import re
from contextlib import AbstractContextManager, contextmanager
from typing import Iterable, NamedTuple, Optional

from .fs import Fs


def _format_block(block_number: int):
    return f'{block_number:010}'


def _parse_range(dirname: str) -> Optional[tuple[int, int]]:
    m = re.match(r'^(\d{10})-(\d{10})$', dirname)
    if m:
        beg = int(m[1])
        end = int(m[2])
        return beg, end


class DataChunk(NamedTuple):
    first_block: int
    last_block: int
    top: int

    def path(self):
        return f'{_format_block(self.top)}/{_format_block(self.first_block)}-{_format_block(self.last_block)}'


def get_tops(fs: Fs) -> list[int]:
    top_dirs = fs.ls()
    tops = [int(d) for d in top_dirs if re.match(r'^\d{10}$', d)]
    tops.sort()
    return tops


def _get_top_ranges(fs: Fs, top: int) -> list[tuple[int, int]]:
    ranges = [r for r in (_parse_range(d) for d in fs.ls(_format_block(top))) if r]
    ranges.sort()
    return ranges


class InvalidLayoutException(Exception):
    pass


def validate_layout(fs: Fs):
    tops = get_tops(fs)

    for i, top in enumerate(tops):
        ranges = _get_top_ranges(fs, top)

        for j, (beg, end) in enumerate(ranges):
            chunk = DataChunk(beg, end, top)

            if beg > end:
                raise InvalidLayoutException(
                    f'invalid data chunk {chunk.path()}: {beg} > {end}'
                )

            if beg < top:
                raise InvalidLayoutException(
                    f'invalid data chunk {chunk.path()}: {top} > {beg}'
                )

            if j > 0 and ranges[j-1][1] >= beg:
                prev_chunk = DataChunk(ranges[j-1][0], ranges[j-1][1], top)
                raise InvalidLayoutException(
                    f'overlapping ranges: {prev_chunk.path()} and {chunk.path()}'
                )

            if i+1 < len(tops) and tops[i+1] <= end:
                raise InvalidLayoutException(
                    f'invalid data chunk {chunk.path()}: range overlaps with {_format_block(tops[i+1])} top dir'
                )


def get_chunks(fs: Fs, first_block: int = 0, last_block: int = math.inf) -> Iterable[DataChunk]:
    assert first_block <= last_block

    tops = get_tops(fs)

    for i, top in enumerate(tops):
        if last_block < top:
            return

        if len(tops) > i + 1 and tops[i + 1] < first_block:
            continue

        for beg, end in _get_top_ranges(fs, top):
            if last_block < beg:
                return

            if first_block > end:
                continue

            yield DataChunk(beg, end, top)


class LayoutConflictException(Exception):
    pass


class ChunkWriter:
    def __init__(self, fs: Fs, first_block: int = 0, last_block: int = math.inf):
        assert last_block >= first_block

        tops = get_tops(fs)
        i = bisect.bisect_left(tops, last_block)

        if i < len(tops) and tops[i] == last_block:
            self._top = last_block
            self._ranges = _get_top_ranges(fs, last_block)
        elif i - 1 < 0:
            self._top = first_block
            self._ranges = []
        else:
            top = tops[i - 1]
            if top >= first_block:
                self._top = top
                self._ranges = _get_top_ranges(fs, top)
            else:
                self._top = first_block
                self._ranges = []

        if self._ranges and self._ranges[-1][1] > last_block:
            overlap = DataChunk(self._ranges[-1][0], self._ranges[-1][1], self._top)
            raise LayoutConflictException(f'chunk {overlap.path()} already exceeds {last_block}. Perhaps part of {first_block}-{last_block} range is controlled by another writer.')

        self._fs = fs
        self.first_block = first_block
        self.last_block = last_block

    def verify_last_chunk(self, last_file: str):
        if not self._ranges:
            return

        beg, end = self._ranges[-1]
        chunk = DataChunk(beg, end, self._top)

        if last_file not in self._fs.ls(chunk.path()):
            self._fs.delete(chunk.path())
            self._ranges.pop()

    @property
    def next_block(self) -> int:
        if self._ranges:
            return self._ranges[-1][1] + 1
        else:
            return self._top

    @contextmanager
    def write(self, first_block: int, last_block: int) -> AbstractContextManager[Fs]:
        assert self.next_block <= first_block <= last_block <= self.last_block

        if len(self._ranges) < 500 or self.last_block == last_block:
            top = self._top
        else:
            top = first_block
            self._ranges = []

        with self._fs.transact(DataChunk(first_block, last_block, top).path()) as loc:
            yield loc

        self._top = top
        self._ranges.append((first_block, last_block))
