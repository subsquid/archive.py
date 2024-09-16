from typing import cast
import pyarrow

from sqa.starknet.writer.model import (WriterBlockStateUpdate, WriterBlock, WriterEvent, WriterTrace,
                                       WriterTransaction)
from sqa.starknet.writer.model import Call as TraceCall
from sqa.writer.parquet import Column, TableBuilder


class BlockTableBuilder(TableBuilder):
    def __init__(self):
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
    def __init__(self):
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

    def append(self, tx: WriterTransaction):
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
    def __init__(self):
        self.block_number = Column(pyarrow.int32())
        self.transaction_index = Column(pyarrow.int32())
        self.event_index = Column(pyarrow.int32())

        self.from_address = Column(pyarrow.string())
        self.key0 = Column(pyarrow.string())
        self.key1 = Column(pyarrow.string())
        self.key2 = Column(pyarrow.string())
        self.key3 = Column(pyarrow.string())
        self.rest_keys = Column(pyarrow.list_(pyarrow.string()))
        self.data = Column(pyarrow.list_(pyarrow.string()))
        self.keys_size = Column(pyarrow.int64())

    def append(self, event: WriterEvent):
        self.block_number.append(event['block_number'])
        self.transaction_index.append(event['transaction_index'])
        self.event_index.append(event['event_index'])
        self.from_address.append(event['from_address'])
        self._append_keys(event['keys'])
        self.data.append(event['data'])

    def _append_keys(self, keys: list[str]) -> None:
        for i in range(4):
            col = getattr(self, f'key{i}')
            col.append(keys[i] if i < len(keys) else None)
        if len(keys) > 4:
            self.rest_keys.append(keys[4:])
        else:
            self.rest_keys.append(None)
        self.keys_size.append(sum(len(k) for k in keys))


class TraceTableBuilder(TableBuilder):
    def __init__(self):
        self.block_number = Column(pyarrow.int32())
        self.transaction_index = Column(pyarrow.int32())
        self.trace_type = Column(pyarrow.string())

        self.invocation_type = Column(pyarrow.string())
        self.call_index = Column(pyarrow.int32())
        self.parent_index = Column(pyarrow.int32())
        self.caller_address = Column(pyarrow.string())
        self.call_contract_address = Column(pyarrow.string())
        self.call_type = Column(pyarrow.string())
        self.call_class_hash = Column(pyarrow.string())
        self.call_entry_point_selector = Column(pyarrow.string())
        self.call_entry_point_type = Column(pyarrow.string())
        self.call_revert_reason = Column(pyarrow.string())
        self.calldata = Column(pyarrow.list_(pyarrow.string()))
        self.call_result = Column(pyarrow.list_(pyarrow.string()))

        self.call_events_keys = Column(pyarrow.list_(pyarrow.string()))
        self.call_events_data = Column(pyarrow.list_(pyarrow.string()))
        self.call_events_order = Column(pyarrow.list_(pyarrow.int32()))

        self.call_messages_payload = Column(pyarrow.list_(pyarrow.string()))
        self.call_messages_from_address = Column(pyarrow.list_(pyarrow.string()))
        self.call_messages_to_address = Column(pyarrow.list_(pyarrow.string()))
        self.call_messages_order = Column(pyarrow.list_(pyarrow.int32()))

    def append_root(self, tx_trace: WriterTrace):
        trace_root = tx_trace['trace_root']
        # NOTE: ensure one and only one call trace presented
        call_index = 0
        if 'execute_invocation' in trace_root:
            call_index += self.append_call(tx_trace, trace_root['execute_invocation'], 'execute_invocation', call_index)
        if 'constructor_invocation' in trace_root:
            call_index += self.append_call(tx_trace, trace_root['constructor_invocation'], 'constructor_invocation', call_index)
        if 'validate_invocation' in trace_root:
            call_index += self.append_call(tx_trace, trace_root['validate_invocation'], 'validate_invocation', call_index)
        if 'fee_transfer_invocation' in trace_root:
            call_index += self.append_call(tx_trace, trace_root['fee_transfer_invocation'], 'fee_transfer_invocation', call_index)

    def append_call(self, tx_trace: WriterTrace, trace_call: TraceCall, invocation_type: str, call_index: int, parent_index: int = -1) -> int:
        # TODO: remake joins for something more vital

        self.block_number.append(tx_trace['block_number'])
        self.transaction_index.append(tx_trace['transaction_index'])
        self.trace_type.append(tx_trace['trace_root']['type'])
        self.invocation_type.append(invocation_type)

        self.parent_index.append(parent_index)
        self.call_index.append(call_index)

        # NOTE: nothing here for reverted calls so all fields NotRequired
        self.caller_address.append(trace_call.get('caller_address'))
        self.call_contract_address.append(trace_call.get('contract_address'))
        self.call_type.append(trace_call.get('call_type'))
        self.call_class_hash.append(trace_call.get('class_hash'))
        self.call_entry_point_selector.append(trace_call.get('entry_point_selector'))
        self.call_entry_point_type.append(trace_call.get('entry_point_type'))
        self.call_revert_reason.append(trace_call.get('revert_reason'))
        self.calldata.append(trace_call.get('calldata'))
        self.call_result.append(trace_call.get('result'))

        # TODO: should call events have own table?
        self.call_events_keys.append([';'.join(e['keys']) for e in trace_call.get('events', [])])
        self.call_events_data.append([';'.join(e['data']) for e in trace_call.get('events', [])])
        self.call_events_order.append([e['order'] for e in trace_call.get('events', [])])

        self.call_messages_payload.append([';'.join(m['payload']) for m in trace_call.get('messages', [])])
        self.call_messages_from_address.append([m['from_address'] for m in trace_call.get('messages', [])])
        self.call_messages_to_address.append([m['to_address'] for m in trace_call.get('messages', [])])
        self.call_messages_order.append([m['order'] for m in trace_call.get('messages', [])])

        # NOTE: Call execution resources omitted

        node_index = call_index + 1
        for t in trace_call.get('calls', []):
            node_index += self.append_call(tx_trace, t, invocation_type, node_index, call_index)

        return node_index - call_index


class StateUpdateTableBuilder(TableBuilder):
    def __init__(self):
        self.block_number = Column(pyarrow.int32())

        self.block_hash = Column(pyarrow.string())
        self.new_root = Column(pyarrow.string())
        self.old_root = Column(pyarrow.string())

        self.storage_diffs_address = Column(pyarrow.list_(pyarrow.string()))
        self.storage_diffs_keys = Column(pyarrow.list_(pyarrow.string()))
        self.storage_diffs_values = Column(pyarrow.list_(pyarrow.string()))
        self.deprecated_declared_classes = Column(pyarrow.list_(pyarrow.string()))
        self.declared_classes_class_hash = Column(pyarrow.list_(pyarrow.string()))
        self.declared_classes_compiled_class_hash = Column(pyarrow.list_(pyarrow.string()))
        self.deployed_contracts_address = Column(pyarrow.list_(pyarrow.string()))
        self.deployed_contracts_class_hash = Column(pyarrow.list_(pyarrow.string()))
        self.replaced_classes_contract_address = Column(pyarrow.list_(pyarrow.string()))
        self.replaced_classes_class_hash = Column(pyarrow.list_(pyarrow.string()))
        self.nonces_contract_address = Column(pyarrow.list_(pyarrow.string()))
        self.nonces_nonce = Column(pyarrow.list_(pyarrow.string()))

    def append(self, state_update: WriterBlockStateUpdate):
        self.block_number.append(state_update['block_number'])

        self.block_hash.append(state_update['block_hash'])
        self.new_root.append(state_update.get('new_root'))
        self.old_root.append(state_update.get('old_root'))

        state_diff = state_update['state_diff']
        self.storage_diffs_address.append([d['address'] for d in state_diff['storage_diffs']])
        # TODO: probably replace with map array
        self.storage_diffs_keys.append([';'.join(e['value'] for e in d['storage_entries']) for d in state_diff['storage_diffs']])
        self.storage_diffs_values.append([';'.join(e['value'] for e in d['storage_entries']) for d in state_diff['storage_diffs']])
        self.deprecated_declared_classes.append(state_diff['deprecated_declared_classes'])
        self.declared_classes_class_hash.append([d['class_hash'] for d in state_diff['declared_classes']])
        self.declared_classes_compiled_class_hash.append([d['compiled_class_hash'] for d in state_diff['declared_classes']])
        self.deployed_contracts_address.append([d['address'] for d in state_diff['deployed_contracts']])
        self.deployed_contracts_class_hash.append([d['class_hash'] for d in state_diff['deployed_contracts']])
        self.replaced_classes_contract_address.append([d['contract_address'] for d in state_diff['replaced_classes']])
        self.replaced_classes_class_hash.append([d['class_hash'] for d in state_diff['replaced_classes']])
        self.nonces_contract_address.append([d['contract_address'] for d in state_diff['nonces']])
        self.nonces_nonce.append([d['nonce'] for d in state_diff['nonces']])
