from typing import Optional

from sqa.query.model import Query


def with_range(q: Query, first_block: int = 0, last_block: Optional[int] = None) -> Query:
    q = dict(q)
    q['fromBlock'] = first_block
    if last_block is None:
        del q['toBlock']
    else:
        q['toBlock'] = last_block
    return q


ETH = 's3://etha-mainnet'
ETH_LOCAL_RANGE = (17090380, 17098719)


GRAVATAR: Query = {
    "fromBlock": 0,
    "logs": [
        {
            "address": ["0x2e645469f354bb4f5c8a05b3b30a929361cf77ec"],
            "topic0": [
                "0x9ab3aefb2ba6dc12910ac1bce4692cf5c3c0d06cff16327c64a3ef78228b130b",
                "0x76571b7a897a1509c641587568218a290018fbdc8b9a724f17b77ff0eec22c0c"
            ],
            "transaction": True
        }
    ],
    "fields": {
        "log": {
            "topics": True,
            "data": True
        },
        "transaction": {
            "from": True,
            "hash": True
        }
    }
}


USDC_TRANSFERS: Query = {
    'fromBlock': 0,
    'fields': {
        'log': {
            'topics': True,
            'data': True
        },
        'transaction': {
            'from': True,
            'hash': True
        }
    },
    'logs': [
        {
            'address': ['0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'],
            'topic0': ['0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef']
        }
    ]
}
