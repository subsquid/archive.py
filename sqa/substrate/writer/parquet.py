import json
from typing import Any

import pyarrow

from sqa.fs import Fs
from sqa.writer.parquet import TableBuilder, Column, BaseParquetWriter, add_size_column, add_index_column
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
        self._ethereum_transact_to = Column(binary())
        self._ethereum_transact_sighash = Column(binary())

    def append(self, block_number: int, call: Call) -> None:
        self.block_number.append(block_number)
        self.extrinsic_index.append(call['extrinsicIndex'])
        self.address.append(call['address'])
        self.name.append(call['name'])
        self.args.append(_to_json(call.get('args')))
        self.origin.append(_to_json(call.get('origin')))
        self.error.append(_to_json(call.get('error')))
        self.success.append(call['success'])
        self._ethereum_transact_to.append(call.get('_ethereumTransactTo'))
        self._ethereum_transact_sighash.append(call.get('_ethereumTransactSighash'))


class EventTable(TableBuilder):
    def __init__(self):
        self.block_number = Column(pyarrow.int32())
        self.index = Column(pyarrow.int32())
        self.name = Column(pyarrow.string())
        self.args = Column(JSON())
        self.phase = Column(pyarrow.string())
        self.extrinsic_index = Column(pyarrow.int32())
        self.call_address = Column(address())
        self.topics = Column(pyarrow.list_(binary()))
        self._evm_log_address = Column(binary())
        self._evm_log_topic0 = Column(binary())
        self._evm_log_topic1 = Column(binary())
        self._evm_log_topic2 = Column(binary())
        self._evm_log_topic3 = Column(binary())
        self._contract_address = Column(binary())
        self._gear_program_id = Column(binary())
        self._revive_contract = Column(binary())
        self._revive_topic0 = Column(binary())
        self._revive_topic1 = Column(binary())
        self._revive_topic2 = Column(binary())
        self._revive_topic3 = Column(binary())

    def append(self, block_number: int, event: Event) -> None:
        self.block_number.append(block_number)
        self.index.append(event['index'])
        self.name.append(event['name'])
        self.args.append(_to_json(event.get('args')))
        self.phase.append(event['phase'])
        self.extrinsic_index.append(event.get('extrinsicIndex'))
        self.call_address.append(event.get('callAddress'))
        self.topics.append(event['topics'])
        self._evm_log_address.append(event.get('_evmLogAddress'))

        evm_topics = iter(event.get('_evmLogTopics', ()))
        self._evm_log_topic0.append(next(evm_topics, None))
        self._evm_log_topic1.append(next(evm_topics, None))
        self._evm_log_topic2.append(next(evm_topics, None))
        self._evm_log_topic3.append(next(evm_topics, None))

        self._contract_address.append(event.get('_contractAddress'))
        self._gear_program_id.append(event.get('_gearProgramId'))

        revive_topics = iter(event.get('_reviveTopics', ()))
        self._revive_contract.append(event.get('_reviveContract'))
        self._revive_topic0.append(next(revive_topics, None))
        self._revive_topic1.append(next(revive_topics, None))
        self._revive_topic2.append(next(revive_topics, None))
        self._revive_topic3.append(next(revive_topics, None))


class ParquetWriter(BaseParquetWriter):
    def __init__(self):
        self.blocks = BlockTable()
        self.extrinsics = ExtrinsicTable()
        self.calls = CallTable()
        self.events = EventTable()

    def push(self, block: Block) -> None:
        block_number = block['header']['height']

        self.blocks.append(block['header'])

        for ex in block.get('extrinsics', ()):
            self.extrinsics.append(block_number, ex)

        for call in block.get('calls', ()):
            self.calls.append(block_number, call)

        for event in block.get('events', ()):
            self.events.append(block_number, event)

    def _write(self, fs: Fs, tables: dict[str, pyarrow.Table]) -> None:
        write_parquet(fs, tables)

    def get_block_height(self, block: Block) -> int:
        return block['header']['height']

    def get_block_hash(self, block: Block) -> str:
        return block['header']['hash']

    def get_block_parent_hash(self, block: Block) -> str:
        return block['header']['parentHash']

    def get_block_timestamp(self, block: Block) -> int:
        if block['header'].get('timestamp') is None:
            return 0
        return int(block['header']['timestamp'] / 1000)


def write_parquet(fs: Fs, tables: dict[str, pyarrow.Table]) -> None:
    kwargs = {
        'data_page_size': 128 * 1024,
        'dictionary_pagesize_limit': 128 * 1024,
        'compression': 'zstd',
        'write_page_index': True,
        'write_batch_size': 100
    }

    events = tables['events']
    events = events.sort_by([
        ('name', 'ascending'),
        ('_evm_log_address', 'ascending'),
        ('_evm_log_topic0', 'ascending'),
        ('_contract_address', 'ascending'),
        ('_gear_program_id', 'ascending'),
        ('block_number', 'ascending'),
        ('index', 'ascending')
    ])
    events = add_size_column(events, 'args')
    events = add_index_column(events)

    fs.write_parquet(
        'events.parquet',
        events,
        row_group_size=20_000,
        use_dictionary=[
            'name',
            'phase',
            '_evm_log_address',
            '_evm_log_topic0',
            '_contract_address',
            '_gear_program_id'
        ],
        write_statistics=[
            'block_number',
            'index',
            'extrinsic_index',
            'name',
            '_evm_log_address',
            '_evm_log_topic0',
            '_contract_address',
            '_gear_program_id',
            '_idx'
        ],
        **kwargs
    )

    calls = tables['calls']
    calls = calls.sort_by([
        ('name', 'ascending'),
        ('_ethereum_transact_to', 'ascending'),
        ('_ethereum_transact_sighash', 'ascending'),
        ('block_number', 'ascending'),
        ('extrinsic_index', 'ascending')
    ])
    calls = add_size_column(calls, 'args')
    calls = add_index_column(calls)

    fs.write_parquet(
        'calls.parquet',
        calls,
        row_group_size=20_000,
        use_dictionary=[
            'name',
            '_ethereum_transact_to',
            '_ethereum_transact_sighash'
        ],
        write_statistics=[
            'block_number',
            'extrinsic_index',
            'name',
            '_ethereum_transact_to',
            '_ethereum_transact_sighash',
            '_idx'
        ],
        **kwargs
    )

    extrinsics = tables['extrinsics']
    extrinsics = add_index_column(extrinsics)

    fs.write_parquet(
        'extrinsics.parquet',
        extrinsics,
        use_dictionary=False,
        write_statistics=[
            'block_number',
            'index',
            'version',
            '_idx'
        ],
        **kwargs
    )

    blocks = tables['blocks']

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
