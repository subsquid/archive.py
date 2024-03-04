from sqa.starknet.writer.model import Block, Event, Transaction
from sqa.writer.parquet import Column, TableBuilder
import pyarrow

class BlockTableBuilder(TableBuilder):
    def __init__(self):
        self.number = Column(pyarrow.int32())
        self.block_hash = Column(pyarrow.string())
        self.parent_hash = Column(pyarrow.string())
        self.timestamp = Column(pyarrow.timestamp('s'))
        self.sequencer_address = Column(pyarrow.string())
        self.new_root = Column(pyarrow.string())
        self.status = Column(pyarrow.string())

    def append(self, block: Block) -> None:
        self.number.append(block['number'])
        self.block_hash.append(block['block_hash'])
        self.parent_hash.append(block['parent_hash'])
        self.timestamp.append(block['timestamp'])
        self.sequencer_address.append(block['sequencer_address'])
        self.new_root.append(block['new_root'])
        self.status.append(block.get('status', ''))


class TxTableBuilder(TableBuilder):
    def __init__(self):
        self.block_number = Column(pyarrow.int32())
        self.transaction_hash = Column(pyarrow.string())
        self.contract_address = Column(pyarrow.string())
        self.entry_point_selector = Column(pyarrow.string())
        self.calldata = Column(pyarrow.list_(pyarrow.string()))
        self.type = Column(pyarrow.string())
        self.max_fee = Column(pyarrow.int64())
        self.version = Column(pyarrow.int32())
        self.signature = Column(pyarrow.list_(pyarrow.string()))
        self.nonce = Column(pyarrow.int64())

    def append(self, tx: Transaction):
        self.block_number.append(tx['block_number'])
        self.transaction_hash.append(tx['transaction_hash'])
        self.contract_address.append(tx.get('contract_address', ''))
        self.entry_point_selector.append(tx.get('entry_point_selector', ''))
        self.calldata.append(tx.get('calldata', []))
        self.type.append(tx.get('type', ''))
        self.max_fee.append(int(tx.get('max_fee') or '0x0', 16))
        self.version.append(int(tx.get('version') or '0x0', 16))
        self.signature.append(tx.get('signature', []))
        self.nonce.append(int(tx.get('nonce') or '0x0', 16))


class LogTableBuilder(TableBuilder):
    def __init__(self):
        self.from_address = Column(pyarrow.string())
        self.keys = Column(pyarrow.list_(pyarrow.string()))
        self.data = Column(pyarrow.list_(pyarrow.string()))
        self.block_hash = Column(pyarrow.string())
        self.block_number = Column(pyarrow.int32())
        self.transaction_hash = Column(pyarrow.string())

    def append(self, event: Event):
        self.from_address.append(event['from_address'])
        self.keys.append(event['keys'])
        self.data.append(event['data'])
        self.block_hash.append(event['block_hash'])
        self.block_number.append(event['block_number'])
        self.transaction_hash.append(event['transaction_hash'])
