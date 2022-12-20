import argparse
import asyncio
import json

from etha.query.model import Query
from etha.task import ipfs_service
from etha.task.query import execute_query


async def main():
    program = argparse.ArgumentParser(
        description='Subsquid network eth archive query task'
    )

    program.add_argument(
        'type',
        choices=['text', 'ipfs'],
        help='Type of the query argument (either inline json or IPFS CID of the query)'
    )

    program.add_argument(
        'query',
        help='Data query'
    )

    program.add_argument(
        'chunks',
        nargs='*',
        help='IPFS CIDs of data chunks to query'
    )

    args = program.parse_args()

    if not args.chunks:
        return

    q = await read_query(args.type, args.query)

    await execute_query(q, args.chunks)

    cid = await ipfs_service.get_cid()
    print(cid)


async def read_query(src_kind: str, src: str) -> Query:
    if src_kind == 'text':
        return json.loads(src)
    elif src_kind == 'ipfs':
        path = await ipfs_service.get_cache(src)
        with open(path) as f:
            return json.load(f)
    else:
        raise TypeError(f'unknown src kind: {src_kind}')


if __name__ == '__main__':
    asyncio.run(main())
