from queries import ETH
from tools import WorkerUser


TestSquid = WorkerUser(
    'TestSquid',
    dataset=ETH,
    query={
        'fromBlock': 16143005,
        'toBlock': 16149151,
        'fields': {
            'block': {
                'number': True,
                'hash': True,
                'parentHash': True,
            },
            'log': {
                'topics': True,
                'data': True,
                # 'transaction': True
            },
            'transaction': {
                'from': True
            }
        },
        'logs': [
            {
                'address': ['0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'],
                'topic0': ['0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef']
            }
        ],
        # 'transactions': [
        #     # {
        #     #     'to': ['0x3883f5e181fccaf8410fa61e12b59bad963fb645']
        #     # }
        # ]
    }
)
