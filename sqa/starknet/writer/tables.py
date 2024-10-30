import pyarrow

from sqa.starknet.writer.model import (
    WriterBlock,
    WriterBlockStateUpdate,
    WriterCall,
    WriterEvent,
    WriterStorageDiffItem,
    WriterTransaction,
)
from sqa.writer.parquet import Column, TableBuilder


class BlockTableBuilder(TableBuilder):
    def __init__(self) -> None:
        self.number = Column(pyarrow.int32())
        self.hash = Column(pyarrow.string())
        self.parent_hash = Column(pyarrow.string())

        self.status = Column(pyarrow.string())
        self.new_root = Column(pyarrow.string())
        self.timestamp = Column(pyarrow.timestamp('s'))
        self.sequencer_address = Column(pyarrow.string())
        self.starknet_version = Column(pyarrow.string())
        self.l1_gas_price_in_fri = Column(pyarrow.string())
        self.l1_gas_price_in_wei = Column(pyarrow.string())

    def append(self, block: WriterBlock) -> None:
        self.number.append(block['number'])
        self.hash.append(block['hash'])
        self.parent_hash.append(block['parent_hash'])
        self.status.append(block['status'])
        self.new_root.append(block['new_root'])
        self.timestamp.append(block['timestamp'])
        self.sequencer_address.append(block['sequencer_address'])
        self.starknet_version.append(block['starknet_version'])
        self.l1_gas_price_in_fri.append(block['l1_gas_price']['price_in_fri'])
        self.l1_gas_price_in_wei.append(block['l1_gas_price']['price_in_wei'])


class TxTableBuilder(TableBuilder):
    def __init__(self) -> None:
        self.block_number = Column(pyarrow.int32())
        self.transaction_index = Column(pyarrow.int32())

        self.transaction_hash = Column(pyarrow.string())
        self.contract_address = Column(pyarrow.string())
        self.entry_point_selector = Column(pyarrow.string())
        self.calldata = Column(pyarrow.list_(pyarrow.string()))
        self.max_fee = Column(pyarrow.string())
        self.type = Column(pyarrow.string())
        self.sender_address = Column(pyarrow.string())
        self.version = Column(pyarrow.string())
        self.signature = Column(pyarrow.list_(pyarrow.string()))
        self.nonce = Column(pyarrow.int32())
        self.class_hash = Column(pyarrow.string())
        self.compiled_class_hash = Column(pyarrow.string())
        self.contract_address_salt = Column(pyarrow.string())
        self.constructor_calldata = Column(pyarrow.list_(pyarrow.string()))
        # TODO: 6 Fields that havent been received left unmatched for a moment

    def append(self, tx: WriterTransaction) -> None:
        self.block_number.append(tx['block_number'])
        self.transaction_index.append(tx['transaction_index'])
        self.transaction_hash.append(tx['transaction_hash'])
        self.contract_address.append(tx.get('contract_address'))
        self.entry_point_selector.append(tx.get('entry_point_selector'))
        self.calldata.append(tx.get('calldata'))
        self.max_fee.append(tx.get('max_fee'))
        self.version.append(tx.get('version'))
        self.signature.append(tx.get('signature'))
        # TODO: if nonce none how would >/< filters will work
        nonce_value = tx.get('nonce')
        self.nonce.append(int(nonce_value, 16) if nonce_value is not None else None)
        self.type.append(tx['type'])
        self.sender_address.append(tx.get('sender_address'))
        self.class_hash.append(tx.get('class_hash'))
        self.compiled_class_hash.append(tx.get('compiled_class_hash'))
        self.contract_address_salt.append(tx.get('contract_address_salt'))
        self.constructor_calldata.append(tx.get('constructor_calldata'))


class EventTableBuilder(TableBuilder):

    MAX_KEYS = 4

    def __init__(self) -> None:
        self.block_number = Column(pyarrow.int32())
        self.transaction_index = Column(pyarrow.int32())
        self.event_index = Column(pyarrow.int32())

        self.trace_address = Column(pyarrow.list_(pyarrow.int32()))

        self.from_address = Column(pyarrow.string())
        self.key0 = Column(pyarrow.string())
        self.key1 = Column(pyarrow.string())
        self.key2 = Column(pyarrow.string())
        self.key3 = Column(pyarrow.string())
        self.rest_keys = Column(pyarrow.list_(pyarrow.string()))
        self.data = Column(pyarrow.list_(pyarrow.string()))
        self.keys_size = Column(pyarrow.int64())

    def append(self, event: WriterEvent) -> None:
        self.block_number.append(event['block_number'])
        self.transaction_index.append(event['transaction_index'])
        self.event_index.append(event['event_index'])
        self.trace_address.append([])
        self.from_address.append(event['from_address'])
        self._append_keys(event['keys'])
        self.data.append(event['data'])

    def append_from_trace(self, tx_trace: WriterCall) -> None:
        # TODO: ensure logic
        if 'revert_reason' in tx_trace:
            return
        for event in tx_trace.get('events', []):
            self.block_number.append(tx_trace['block_number'])
            self.transaction_index.append(tx_trace['transaction_index'])
            self.event_index.append(event['order'])
            self.trace_address.append(tx_trace['trace_address'])
            self.from_address.append(tx_trace['contract_address'])
            self._append_keys(event['keys'])
            self.data.append(event['data'])

    def _append_keys(self, keys: list[str]) -> None:
        for i in range(self.MAX_KEYS):
            col = getattr(self, f'key{i}')
            col.append(keys[i] if i < len(keys) else None)
        if len(keys) > self.MAX_KEYS:
            self.rest_keys.append(keys[self.MAX_KEYS:])
        else:
            self.rest_keys.append(None)
        self.keys_size.append(sum(len(k) for k in keys))


class TraceTableBuilder(TableBuilder):
    def __init__(self) -> None:
        self.block_number = Column(pyarrow.int32())
        self.transaction_index = Column(pyarrow.int32())
        self.trace_type = Column(pyarrow.string())
        self.invocation_type = Column(pyarrow.string())

        self.trace_address = Column(pyarrow.list_(pyarrow.int32()))

        self.caller_address = Column(pyarrow.string())
        self.contract_address = Column(pyarrow.string())
        self.call_type = Column(pyarrow.string())
        self.class_hash = Column(pyarrow.string())
        self.entry_point_selector = Column(pyarrow.string())
        self.entry_point_type = Column(pyarrow.string())
        self.call_revert_reason = Column(pyarrow.string())
        self.calldata = Column(pyarrow.list_(pyarrow.string()))
        self.call_result = Column(pyarrow.list_(pyarrow.string()))

    def append_call(self, tx_trace: WriterCall) -> None:
        self.block_number.append(tx_trace['block_number'])
        self.transaction_index.append(tx_trace['transaction_index'])
        self.trace_type.append(tx_trace['trace_type'])
        self.invocation_type.append(tx_trace['invocation_type'])

        self.trace_address.append(tx_trace['trace_address'])

        # NOTE: fields below are empty for reverted calls
        self.caller_address.append(tx_trace.get('caller_address'))
        self.contract_address.append(tx_trace.get('contract_address'))
        self.call_type.append(tx_trace.get('call_type'))
        self.class_hash.append(tx_trace.get('class_hash'))
        self.entry_point_selector.append(tx_trace.get('entry_point_selector'))
        self.entry_point_type.append(tx_trace.get('entry'))
        self.call_revert_reason.append(tx_trace.get('revert_reason'))
        self.calldata.append(tx_trace.get('calldata'))
        self.call_result.append(tx_trace.get('result'))

        # NOTE: Call execution resources omitted


class MessageTableBuilder(TableBuilder):
    def __init__(self) -> None:
        self.block_number = Column(pyarrow.int32())
        self.transaction_index = Column(pyarrow.int32())
        self.trace_address = Column(pyarrow.list_(pyarrow.int32()))

        self.from_address = Column(pyarrow.string())
        self.to_address = Column(pyarrow.string())
        self.payload = Column(pyarrow.list_(pyarrow.string()))
        self.order = Column(pyarrow.int32())

    def append_from_call(self, trace_call: WriterCall) -> None:
        if 'revert_reason' in trace_call:
            return
        for m in trace_call['messages']:
            self.block_number.append(trace_call['block_number'])
            self.transaction_index.append(trace_call['transaction_index'])
            self.trace_address.append(trace_call['trace_address'])

            self.from_address.append(m['from_address'])
            self.to_address.append(m['to_address'])
            self.payload.append(m['payload'])
            self.order.append(m['order'])


class StateUpdateTableBuilder(TableBuilder):
    def __init__(self) -> None:
        self.block_number = Column(pyarrow.int32())

        self.new_root = Column(pyarrow.string())
        self.old_root = Column(pyarrow.string())

        self.deprecated_declared_classes = Column(pyarrow.list_(pyarrow.string()))
        self.declared_classes = Column(pyarrow.list_(pyarrow.struct([
            ('class_hash', pyarrow.string()),
            ('compiled_class_hash', pyarrow.string())
        ])))
        self.deployed_contracts = Column(pyarrow.list_(pyarrow.struct([
            ('address', pyarrow.string()),
            ('class_hash', pyarrow.string())
        ])))
        self.replaced_classes = Column(pyarrow.list_(pyarrow.struct([
            ('contract_address', pyarrow.string()),
            ('class_hash', pyarrow.string())
        ])))
        self.nonces = Column(pyarrow.list_(pyarrow.struct([
            ('contract_address', pyarrow.string()),
            ('nonce', pyarrow.string())
        ])))

    def append(self, state_update: WriterBlockStateUpdate) -> None:
        self.block_number.append(state_update['block_number'])

        self.new_root.append(state_update.get('new_root'))
        self.old_root.append(state_update.get('old_root'))

        state_diff = state_update['state_diff']
        self.deprecated_declared_classes.append(state_diff['deprecated_declared_classes'])
        self.declared_classes.append(state_diff['declared_classes'])
        self.deployed_contracts.append(state_diff['deployed_contracts'])
        self.replaced_classes.append(state_diff['replaced_classes'])
        self.nonces.append(state_diff['nonces'])


class StorageDiffTableBuilder(TableBuilder):
    def __init__(self) -> None:
        self.block_number = Column(pyarrow.int32())
        self.address = Column(pyarrow.string())

        self.keys = Column(pyarrow.list_(pyarrow.string()))
        self.values = Column(pyarrow.list_(pyarrow.string()))

    def append(self, storage_diff: WriterStorageDiffItem) -> None:
        self.block_number.append(storage_diff['block_number'])
        self.address.append(storage_diff['address'])

        self.keys.append([e['key'] for e in storage_diff['storage_entries']])
        self.values.append([e['value'] for e in storage_diff['storage_entries']])
