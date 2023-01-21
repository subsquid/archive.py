from queries import TEST, ETH
from tools import WorkerUser


TestSquid = WorkerUser(
    'TestSquid',
    dataset=ETH,
    query=TEST
)
