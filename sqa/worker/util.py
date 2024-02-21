import hashlib


def sha3_256(data: bytes) -> bytes:
    hasher = hashlib.sha3_256()
    hasher.update(data)
    return hasher.digest()
