from tools import WorkerUser


GravatarSquid = WorkerUser(
    'GravatarSquid',
    dataset='s3://etha-mainnet-sia',
    query={
        "fromBlock": 16143005,
        "toBlock": 16218816,
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
            },
            "transaction": {}
        },
        "transactions": []
    }
)
