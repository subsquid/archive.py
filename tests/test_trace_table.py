import pytest

from sqa.eth.ingest.tables import TraceTableBuilder


def _rows(b: TraceTableBuilder) -> int:
    return len(b.type.buf)


def test_debug_append_skips_typeless_error_frame():
    # A node may return a typeless error object (or {}) for an untraceable tx.
    # The writer must skip it, not crash the whole dump (KeyError: 'type').
    for bad in ({}, {'error': 'execution reverted'}):
        b = TraceTableBuilder()
        b.debug_append('0x13cd9fc', '0x0', bad)
        assert _rows(b) == 0


def test_debug_append_records_valid_call_frame():
    b = TraceTableBuilder()
    frame = {
        'type': 'CALL',
        'from': '0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
        'to': '0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb',
        'value': '0x0',
        'gas': '0x1',
        'gasUsed': '0x1',
        'input': '0xa9059cbb',
    }
    b.debug_append('0x13cd9fc', '0x0', frame)
    assert _rows(b) == 1
    assert b.type.buf == ['call']


def test_debug_append_still_raises_on_unknown_nonnull_type():
    b = TraceTableBuilder()
    with pytest.raises(Exception):
        b.debug_append('0x13cd9fc', '0x0', {'type': 'BOGUS'})
