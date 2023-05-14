import bisect
import math
import re
from contextlib import AbstractContextManager, contextmanager
from typing import Iterable, NamedTuple, Optional, Callable

from etha.fs import Fs


def _format_block(block_number: int):
    return f'{block_number:010}'


def _parse_range(dirname: str) -> Optional[tuple[int, int, str]]:
    m = re.match(r'^(\d{10})-(\d{10})-(\w{8})$', dirname)
    if m:
        beg = int(m[1])
        end = int(m[2])
        hash_ = m[3]
        return beg, end, hash_


class DataChunk(NamedTuple):
    first_block: int
    last_block: int
    last_hash: str
    top: int

    def path(self):
        return f'{_format_block(self.top)}/{_format_block(self.first_block)}-{_format_block(self.last_block)}-{self.last_hash}'


def get_tops(fs: Fs) -> list[int]:
    top_dirs = fs.ls()
    tops = [int(d) for d in top_dirs if re.match(r'^\d{10}$', d)]
    tops.sort()
    return tops


def _get_top_ranges(fs: Fs, top: int) -> list[tuple[int, int, str]]:
    ranges = [r for r in (_parse_range(d) for d in fs.ls(_format_block(top))) if r]
    ranges.sort()
    return ranges


class InvalidLayoutException(Exception):
    pass


def validate_layout(fs: Fs):
    tops = get_tops(fs)

    for i, top in enumerate(tops):
        ranges = _get_top_ranges(fs, top)

        for j, (beg, end, hash_) in enumerate(ranges):
            chunk = DataChunk(beg, end, hash_, top)

            if beg > end:
                raise InvalidLayoutException(
                    f'invalid data chunk {chunk.path()}: {beg} > {end}'
                )

            if beg < top:
                raise InvalidLayoutException(
                    f'invalid data chunk {chunk.path()}: {top} > {beg}'
                )

            if j > 0 and ranges[j-1][1] >= beg:
                prev_chunk = DataChunk(ranges[j-1][0], ranges[j-1][1], ranges[j-1][2], top)
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

        for beg, end, hash_ in _get_top_ranges(fs, top):
            if last_block < beg:
                return

            if first_block > end:
                continue

            yield DataChunk(beg, end, hash_, top)


def get_chunks_in_reversed_order(fs: Fs, first_block: int = 0, last_block: int = math.inf) -> Iterable[DataChunk]:
    assert first_block <= last_block

    tops = get_tops(fs)

    for top in reversed(tops):
        if top > last_block:
            continue

        for beg, end, hash_ in _get_top_ranges(fs, top):
            if beg > last_block:
                continue
            if end < first_block:
                return
            yield DataChunk(beg, end, hash_, top)


class ChunkWriter:
    def __init__(
            self,
            fs: Fs,
            chunk_check: Callable[[list[str]], bool],
            first_block: int = 0,
            last_block: Optional[int] = None
    ):
        last_block = math.inf if last_block is None else last_block
        assert last_block >= first_block

        self.first_block = first_block
        self.last_block = last_block

        chunks = iter(get_chunks(fs, first_block=first_block, last_block=last_block))
        reversed_chunks = iter(get_chunks_in_reversed_order(fs, first_block=first_block, last_block=last_block))

        first_chunk = next(chunks, None)
        last_chunk = next(reversed_chunks, None)

        if first_chunk and first_chunk.first_block != first_block:
            raise LayoutConflictException(
                f'First chunk of the range {first_block}-{last_block} is {first_chunk}. '
                f'Perhaps part of the range {first_block}-{last_block} is controlled by another writer'
            )

        if last_chunk and last_chunk.last_block > last_block:
            raise LayoutConflictException(
                f'Chunk {last_chunk} is not aligned with the range {first_block}-{last_block}. '
                f'Perhaps part of the range {first_block}-{last_block} is controlled by another writer'
            )

        if last_chunk and not chunk_check(fs.ls(last_chunk.path())):
            fs.delete(last_chunk.path())
            last_chunk = next(reversed_chunks, None)

        if last_chunk:
            self._top = last_chunk.top
            self._ranges = _get_top_ranges(fs, self._top)
        else:
            self._top = first_block
            self._ranges = []

    @property
    def next_block(self) -> int:
        if self._ranges:
            return self._ranges[-1][1] + 1
        else:
            return self._top

    @property
    def last_hash(self) -> str | None:
        if self._ranges:
            return self._ranges[-1][2]
        else:
            return None

    def next_chunk(self, first_block: int, last_block: int, last_hash: str) -> DataChunk:
        assert self.next_block <= first_block <= last_block <= self.last_block

        if len(self._ranges) < 500 or self.last_block == last_block:
            top = self._top
        else:
            top = first_block
            self._ranges = []

        self._top = top
        self._ranges.append((first_block, last_block, last_hash))
        return DataChunk(first_block, last_block, last_hash, top)



class LayoutConflictException(Exception):
    pass
