import pyarrow

from sqa.starknet.writer.model import (WriterBlock, WriterEvent,
                                       WriterTransaction)
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
