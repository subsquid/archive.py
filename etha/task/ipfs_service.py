import os
from typing import AsyncIterable, Iterable, Optional, Union

import httpx


_client = httpx.AsyncClient(
    base_url=os.environ['SQD_IPFS_SERVICE'],
    timeout=600
)


async def get_cache(cid: str) -> str:
    res = await _client.get(f'cache/{cid}')
    res.raise_for_status()
    loc = os.environ.get('SQD_IPFS_CACHE_DIR', '/ipfs')
    return os.path.join(loc, res.text)


async def publish(path: str, content: Union[str, bytes, Iterable[bytes], AsyncIterable[bytes]]):
    tid = os.environ['SQD_TASK_ID']
    assert tid
    p = '/fs/' + tid
    if path:
        p += '/' + path
    res = await _client.post(p, content=content)
    res.raise_for_status()


async def get_cid(path: Optional[str] = None) -> str:
    tid = os.environ['SQD_TASK_ID']
    assert tid
    p = '/fs/' + tid
    if path:
        p += '/' + path
    res = await _client.get(p)
    res.raise_for_status()
    return res.text

