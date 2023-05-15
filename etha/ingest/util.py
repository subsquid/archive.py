from typing import TypedDict, NotRequired

from etha.ingest.model import Qty, Hash32, Transaction, Address20, DebugFrame


def qty2int(v: Qty) -> int:
    return int(v, 16)


def short_hash(value: Hash32) -> str:
    """Takes first 4 bytes from the hash"""
    assert value.startswith('0x')
    return value[2:10]


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
