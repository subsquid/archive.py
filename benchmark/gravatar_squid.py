from queries import GRAVATAR, with_range
from tools import ArchiveUser


GravatarSquid = ArchiveUser(
    'GravatarSquid',
    query=with_range(GRAVATAR, 0, 7_200_000),
)
