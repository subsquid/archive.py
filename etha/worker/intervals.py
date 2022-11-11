from typing import Iterable, Optional


Range = tuple[int, int]


def difference(a: Iterable[Range], b: Iterable[Range]):
    bit = iter(b)
    br = next(bit, None)

    for ar in a:
        while br and br[1] < ar[0]:
            br = next(bit, None)

        while br and (i := _intersection(ar, br)):
            if ar[0] < i[0]:
                yield ar[0], i[0] - 1

            if i[1] < ar[1]:
                ar = i[1] + 1, ar[1]
                br = next(bit, None)
            else:
                ar = None
                break

        if ar:
            yield ar


def _intersection(a: Range, b: Range) -> Optional[Range]:
    beg = max(a[0], b[0])
    end = min(a[1], b[1])
    if beg <= end:
        return beg, end
    else:
        return None


def union(*sets: Iterable[Range]) -> Iterable[Range]:
    if len(sets) == 0:
        return []
    elif len(sets) == 1:
        return sets[0]
    elif len(sets) == 2:
        return remove_intersections(_order(sets[0], sets[1]))
    else:
        ranges = []
        for rs in sets:
            ranges.extend(rs)
        ranges.sort()
        return remove_intersections(ranges)


def remove_intersections(ordered_ranges: Iterable[Range]) -> Iterable[Range]:
    prev = None
    for r in ordered_ranges:
        if prev:
            if prev[1] + 1 >= r[0]:
                prev = prev[0], max(prev[1], r[1])
            else:
                yield prev
                prev = r
        else:
            prev = r
    if prev:
        yield prev


def _order(a: Iterable[Range], b: Iterable[Range]) -> Iterable[Range]:
    ait = iter(a)
    bit = iter(b)
    ar = next(ait, None)
    br = next(bit, None)
    while True:
        if ar is None:
            if br:
                yield br
                for br in bit:
                    yield br
            return

        if br is None:
            yield ar
            for ar in ait:
                yield ar
            return

        if ar < br:
            yield ar
            ar = next(ait, None)
        else:
            yield br
            br = next(bit, None)
