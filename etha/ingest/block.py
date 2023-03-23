import binascii

from eth.rlp import headers

from etha.ingest.model import Block


def int_to_big_endian(value: int) -> bytes:
    return value.to_bytes((value.bit_length() + 7) // 8 or 1, 'big')


def decode_hex(value: str) -> bytes:
    if value.startswith('0x'):
        value = value[2:]
    ascii_hex = value.encode("ascii")
    return binascii.unhexlify(ascii_hex)


def calculate_hash(block: Block) -> str:
    header = headers.BlockHeader(
        int(block['difficulty'], 16),
        int(block['number'], 16),
        int(block['gasLimit'], 16),
        int(block['timestamp'], 16),
        decode_hex(block['miner']),
        decode_hex(block['parentHash']),
        decode_hex(block['sha3Uncles']),
        decode_hex(block['stateRoot']),
        decode_hex(block['transactionsRoot']),
        decode_hex(block['receiptsRoot']),
        int(block['logsBloom'], 16),
        int(block['gasUsed'], 16),
        decode_hex(block['extraData']),
        decode_hex(block['mixHash']),
        int_to_big_endian(int(block['nonce'], 16)).rjust(8, b'\x00'),
    )
    return header.hex_hash
