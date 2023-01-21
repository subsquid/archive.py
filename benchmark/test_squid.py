from queries import ETH, TEST, with_range
from tools import ArchiveUser


GravatarSquid = ArchiveUser(
    'TestSquid',
    dataset=ETH,
    query=with_range(TEST, 13_000_000, 14_000_000)
)
