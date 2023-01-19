from queries import ETH, with_range, GRAVATAR
from tools import WorkerUser


GravatarSquid = WorkerUser(
    'GravatarSquid',
    dataset=ETH,
    query=with_range(GRAVATAR, 16143005, 16149151)
)
