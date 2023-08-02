import json
from typing import Any

import pyarrow

from sqa.fs import Fs
from sqa.writer.parquet import TableBuilder, Column
from .model import BlockHeader, Extrinsic, Call, BigInt, Event, Block


def bigint():
    return pyarrow.decimal128(38)


def binary():
    return pyarrow.string()


def JSON():
    return pyarrow.string()


def address():
    return pyarrow.list_(pyarrow.uint32())


def _to_json(val: Any) -> str | None:
    if val is None:
        return None
    else:
        return json.dumps(val)


def _to_bigint(val: BigInt | None) -> int | None:
    if val is None:
        return None
    else:
        return int(val)


class BlockTable(TableBuilder):
    def __init__(self):
        self.number = Column(pyarrow.int32())
        self.hash = Column(binary())
        self.parent_hash = Column(binary())
        self.state_root = Column(binary())
        self.extrinsics_root = Column(binary())
        self.digest = Column(JSON())
        self.spec_name = Column(pyarrow.string())
        self.spec_version = Column(pyarrow.uint32())
        self.impl_name = Column(pyarrow.string())
        self.impl_version = Column(pyarrow.uint32())
        self.timestamp = Column(pyarrow.timestamp('ms', tz='UTC'))
        self.validator = Column(pyarrow.string())

    def append(self, block: BlockHeader) -> None:
        self.number.append(block['height'])
        self.hash.append(block['hash'])
        self.parent_hash.append(block['parentHash'])
        self.state_root.append(block['stateRoot'])
        self.extrinsics_root.append(block['extrinsicsRoot'])
        self.digest.append(json.dumps(block['digest']))
        self.spec_name.append(block['specName'])
        self.spec_version.append(block['specVersion'])
        self.impl_name.append(block['implName'])
        self.impl_version.append(block['implVersion'])
        self.timestamp.append(block.get('timestamp'))
        self.validator.append(block.get('validator'))


class ExtrinsicTable(TableBuilder):
    def __init__(self):
        self.block_number = Column(pyarrow.int32())
        self.index = Column(pyarrow.int32())
        self.version = Column(pyarrow.int32())
        self.signature = Column(JSON())
        self.fee = Column(bigint())
        self.tip = Column(bigint())
        self.error = Column(JSON())
        self.success = Column(pyarrow.bool_())
        self.hash = Column(binary())

    def append(self, block_number: int, ex: Extrinsic) -> None:
        self.block_number.append(block_number)
        self.index.append(ex['index'])
        self.version.append(ex['version'])
        self.signature.append(_to_json(ex.get('signature')))
        self.fee.append(_to_bigint(ex.get('fee')))
        self.tip.append(_to_bigint(ex.get('tip')))
        self.error.append(_to_json(ex.get('error')))
        self.success.append(ex['success'])
        self.hash.append(ex['hash'])


class CallTable(TableBuilder):
    def __init__(self):
        self.block_number = Column(pyarrow.int32())
        self.extrinsic_index = Column(pyarrow.int32())
        self.address = Column(address())
        self.name = Column(pyarrow.string())
        self.args = Column(JSON())
        self.origin = Column(JSON())
        self.error = Column(JSON())
        self.success = Column(pyarrow.bool_())

    def append(self, block_number: int, call: Call) -> None:
        self.block_number.append(block_number)
        self.extrinsic_index.append(call['extrinsicIndex'])
        self.address.append(call['address'])
        self.name.append(call['name'])
        self.args.append(_to_json(call.get('args')))
        self.origin.append(_to_json(call.get('origin')))
        self.error.append(_to_json(call.get('error')))
        self.success.append(call['success'])


class EventTable(TableBuilder):
    def __init__(self):
        self.block_number = Column(pyarrow.int32())
        self.index = Column(pyarrow.int32())
        self.name = Column(pyarrow.string())
        self.args = Column(JSON())
        self.phase = Column(pyarrow.string())
        self.extrinsic_index = Column(pyarrow.int32())
        self.call_address = Column(address())

    def append(self, block_number: int, event: Event) -> None:
        self.block_number.append(block_number)
        self.index.append(event['index'])
        self.name.append(event['name'])
        self.args.append(_to_json(event.get('args')))
        self.phase.append(event['phase'])
        self.extrinsic_index.append(event.get('extrinsicIndex'))
        self.call_address.append(event.get('callAddress'))


class ParquetSink:
    def __init__(self):
        self._init()

    def _init(self):
        self.block_table = BlockTable()
        self.extrinsic_table = ExtrinsicTable()
        self.call_table = CallTable()
        self.event_table = EventTable()

    def buffered_bytes(self) -> int:
        return self.block_table.bytesize() \
            + self.extrinsic_table.bytesize() \
            + self.call_table.bytesize() \
            + self.event_table.bytesize()

    def write(self, block: Block) -> None:
        block_number = block['header']['height']

        self.block_table.append(block['header'])

        for ex in block.get('extrinsics', ()):
            self.extrinsic_table.append(block_number, ex)

        for call in block.get('calls', ()):
            self.call_table.append(block_number, call)

        for event in block.get('events', ()):
            self.event_table.append(block_number, event)

    def flush(self, fs: Fs) -> None:
        blocks = self.block_table.to_table()
        extrinsics = self.extrinsic_table.to_table()
        calls = self.call_table.to_table()
        events = self.event_table.to_table()

        calls = calls.sort_by([
            ('name', 'ascending'),
            ('block_number', 'ascending'),
            ('extrinsic_index', 'ascending')
        ])

        events = events.sort_by([
            ('name', 'ascending'),
            ('block_number', 'ascending'),
            ('extrinsic_index', 'ascending')
        ])

        kwargs = {
            'data_page_size': 32 * 1024,
            'compression': 'zstd',
            'compression_level': 12
        }

        fs.write_parquet(
            'events.parquet',
            events,
            use_dictionary=['name', 'phase'],
            write_statistics=[
                'block_number',
                'index',
                'extrinsic_index',
                'name'
            ],
            **kwargs
        )

        fs.write_parquet(
            'calls.parquet',
            calls,
            use_dictionary=['name'],
            write_statistics=[
                'block_number',
                'extrinsic_index',
                'name'
            ],
            **kwargs
        )

        fs.write_parquet(
            'extrinsics.parquet',
            extrinsics,
            use_dictionary=False,
            write_statistics=[
                'block_number',
                'index',
                'version'
            ],
            **kwargs
        )

        fs.write_parquet(
            'blocks.parquet',
            blocks,
            use_dictionary=['spec_name', 'impl_name', 'validator'],
            write_statistics=[
                'number',
                'spec_name',
                'spec_version',
                'impl_name',
                'impl_version',
                'timestamp',
                'validator'
            ],
            **kwargs
        )

        self._init()
