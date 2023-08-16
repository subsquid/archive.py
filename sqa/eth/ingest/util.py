import binascii
from typing import TypedDict, NotRequired

from Crypto.Hash import keccak

from sqa.eth.ingest.model import Qty, Hash32, Transaction, Address20


def qty2int(v: Qty) -> int:
    return int(v, 16)


def short_hash(value: Hash32) -> str:
    """Takes first 4 bytes from the hash"""
    assert value.startswith('0x')
    return value[2:10]


def decode_hex(value: str) -> bytes:
    assert value.startswith('0x')
    ascii_hex = value[2:].encode('ascii')
    return binascii.unhexlify(ascii_hex)


def encode_hex(value: bytes | bytearray) -> str:
    binary_hex = binascii.hexlify(value)
    return '0x' + binary_hex.decode('ascii')


def keccak256(buffer) -> bytes:
    k = keccak.new(digest_bits=256)
    return bytes(k.update(buffer).digest())


class TxStatus(TypedDict):
    status: Qty
    contractAddress: NotRequired[Address20]


def get_tx_status_from_traces(tx: Transaction) -> TxStatus | None:
    if frame_result := tx.get('debugFrame_'):
        frame = frame_result['result']
        ok = frame.get('error') is None and frame['type'] != 'INVALID'
        s: TxStatus = {
            'status': '0x1' if ok else '0x0'
        }
        if ok and frame['type'] in ('CREATE', 'CREATE2'):
            s['contractAddress'] = frame['to']
        return s

    replay = tx.get('traceReplay_')
    if replay is not None and (trace := replay.get('trace')):
        top = trace[0]
        assert top['traceAddress'] == []
        ok = top.get('error') is None
        s: TxStatus = {
            'status': '0x1' if ok else '0x0'
        }
        if ok and top['type'] == 'create':
            s['contractAddress'] = top['result']['address']
        return s


def _add_to_bloom(bloom: bytearray, bloom_entry: bytes):
    hash = keccak256(bloom_entry)
    for idx in (0, 2, 4):
        bit_to_set = int.from_bytes(hash[idx:idx+2], "big") & 0x07FF
        bit_index = 0x07FF - bit_to_set
        byte_index = bit_index // 8
        bit_value = 1 << (7 - (bit_index % 8))
        bloom[byte_index] = bloom[byte_index] | bit_value


def logs_bloom(logs) -> str:
    bloom = bytearray(b"\x00" * 256)
    for log in logs:
        _add_to_bloom(bloom, decode_hex(log['address']))
        for topic in log['topics']:
            _add_to_bloom(bloom, decode_hex(topic))
    return encode_hex(bloom)
