from queries import ETH, GRAVATAR
from tools import ArchiveUser


GravatarSquid = ArchiveUser(
    'GravatarSquid',
    dataset=ETH,
    query=GRAVATAR
)
