from queries import GRAVATAR, with_range
from tools import ArchiveUser


GravatarSquid = ArchiveUser(
    'GravatarSquid',
    query=with_range(GRAVATAR, 0, 6_200_000),
)
