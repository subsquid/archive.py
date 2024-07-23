import json
import logging
import math
import sys
import time
from typing import Iterable, Callable

from . import Block


LOG = logging.getLogger(__name__)


def ingest_from_service(
        service_url: str,
        get_block_height: Callable[[Block], int],
        next_block: int,
        last_block=None
) -> Iterable[Block]:
    import httpx

    if last_block is None:
        last_block = math.inf

    data_range = {
        'from': next_block
    }

    if last_block < math.inf:
        data_range['to'] = last_block

    while data_range['from'] <= last_block:
        try:
            with httpx.stream('POST', service_url, json=data_range, timeout=httpx.Timeout(None)) as res:
                res.raise_for_status()
                for line in _iter_lines(res.iter_text()):
                    block: Block = json.loads(line)
                    height = get_block_height(block)
                    data_range['from'] = height + 1
                    yield block
        except (httpx.NetworkError, httpx.RemoteProtocolError):
            LOG.exception('data streaming error, will pause for 5 sec and try again')
            time.sleep(5)
        else:
            LOG.info('no blocks were found. waiting 5 min for a new try')
            time.sleep(300)


# `res.iter_lines()` uses `str.splitlines()` under the hood, which splits "too much".
# E.g `"\u2028"` is a perfectly valid JSON,
# but it will get split into 2 peaces by `str.splitlines()`.
def _iter_lines(text_stream: Iterable[str]) -> Iterable[str]:
    buf = []

    def prepend_buf(line: str) -> str:
        if buf:
            buf.append(line)
            line = ''.join(buf)
            buf.clear()
            return line
        else:
            return line

    for chunk in text_stream:
        lines = chunk.split('\n')
        if len(lines) > 1:
            lines[0] = prepend_buf(lines[0])
            for i in range(len(lines) - 1):
                yield lines[i]

        buf.append(lines[-1])

    last_line = prepend_buf('')
    if last_line:
        yield last_line


def ingest_from_stdin(
        get_block_height: Callable[[Block], int],
        next_block: int,
        last_block=math.inf
) -> Iterable[Block]:
    for line in sys.stdin:
        if line:
            block: Block = json.loads(line)
            height = get_block_height(block)
            if height > last_block:
                break
            elif next_block <= height:
                yield block
