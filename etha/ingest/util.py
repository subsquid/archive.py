from etha.ingest.model import Qty, Hash32


def qty2int(v: Qty) -> int:
    return int(v, 16)


def trim_hash(value: Hash32) -> str:
    '''Takes first 4 bytes from the hash'''
    assert value.startswith('0x')
    return value[2:10]
