from typing import Optional

from etha.query.model import Query


def with_range(q: Query, first_block: int = 0, last_block: Optional[int] = None) -> Query:
    q = dict(q)
    q['fromBlock'] = first_block
    if last_block is None:
        del q['toBlock']
    else:
        q['toBlock'] = last_block
    return q


ETH = 's3://etha-mainnet-sia'


GRAVATAR: Query = {
    "fromBlock": 0,
    "toBlock": 16000000,
    "logs": [
        {
            "address": ["0x2e645469f354bb4f5c8a05b3b30a929361cf77ec"],
            "topic0": [
                "0x9ab3aefb2ba6dc12910ac1bce4692cf5c3c0d06cff16327c64a3ef78228b130b",
                "0x76571b7a897a1509c641587568218a290018fbdc8b9a724f17b77ff0eec22c0c"
            ]
        }
    ],
    "fields": {
        "block": {
            "number": True,
            "hash": True,
            "parentHash": True,
        },
        "log": {
            "topics": True,
            "data": True
        }
    }
}


TEST: Query = {
    "fromBlock": 0,
    "toBlock": 16000000,
    "transactions": [
        {
            "to": ["0x9cb7712c6a91506e69e8751fcb08e72e1256477d"],
            "sighash": ["0x8ca887ca"]
        }
    ],
    "logs": [
        {
            "address": ["0x0f98431c8ad98523631ae4a59f267346ea31f984"],
            "topic0": ["0x783cca1c0412dd0d695e784568c96da2e9c22ff989357a2e8b1d9b2b4e6b7118"]
        },
        {
            "address": ["0xc36442b4a4522e871399cd717abdd847ab11fe88"],
            "topic0": [
                "0x3067048beee31b25b2f1681f88dac838c8bba36af25bfb2b7cf7473a5847e35f",
                "0x26f6a048ee9138f2c0ce266f322cb99228e8d619ae2bff30c67f8dcf9d2377b4",
                "0x40d0efd1a53d60ecbf40971b9daf7dc90178c3aadc7aab1765632738fa8b8f01",
                "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
            ]
        },
        {
            "topic0": [
                "0x0c396cd989a39f4459b5fa1aed6a9a8dcdbc45908acfd67e028cd568da98982c",
                "0x7a53080ba414158be7ec69b987b5fb7d07dee101fe85488f0853ae16239d0bde",
                "0x98636036cb66a9c19a37435efc1e90142190214e8abeb821bdba3f2990dd4c95",
                "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67"
            ]
        }
    ],
    "fields": {
        "block": {
            "number": True,
            "hash": True,
            "parentHash": True,
        },
        "log": {
            "address": True,
            "topics": True,
            "data": True,
            "transaction": True
        },
        "transaction": {
            "from": True,
            "to": True,
            "gasPrice": True,
            "gas": True
        }
    }
}
