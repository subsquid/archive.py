from tools import WorkerUser


GravatarSquid = WorkerUser(
    'GravatarSquid',
    dataset='s3://etha-mainnet-sia',
    query={
        "fromBlock": 16143005,
        "toBlock": 16218816,
        "logs": [
            {
                "address": ["0x2E645469f354BB4F5c8a05B3b30A929361cf77eC".lower()],
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
        },
        "transactions": []
    }
)
