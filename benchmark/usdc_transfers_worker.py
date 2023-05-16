from queries import ETH, USDC_TRANSFERS, with_range, ETH_LOCAL_RANGE
from tools import WorkerUser


USDCTransfersSquid = WorkerUser(
    'USDCTransfersSquid',
    dataset=ETH,
    query=with_range(USDC_TRANSFERS, *ETH_LOCAL_RANGE)
)
