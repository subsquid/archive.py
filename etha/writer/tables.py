import pyarrow

from etha.writer.column import Column
from etha.writer.model import BlockHeader, Transaction, Log


def bignum():
    return pyarrow.decimal128(38)


def access_list(tx: Transaction):
    if tx['accessList'] is not None:
        access_list = []
        for item in tx['accessList']:
            access_list.append({
                'address': item['address'],
                'storage_keys': item['storageKeys']
            })
        return access_list


class BlockTableBuilder:
    def __init__(self):
        self.number = Column(pyarrow.int32())
        self.hash = Column(pyarrow.string())
        self.parent_hash = Column(pyarrow.string())
        self.nonce = Column(pyarrow.string())
        self.sha3_uncles = Column(pyarrow.string())
        self.logs_bloom = Column(pyarrow.string())
        self.transactions_root = Column(pyarrow.string())
        self.state_root = Column(pyarrow.string())
        self.receipts_root = Column(pyarrow.string())
        self.miner = Column(pyarrow.string())
        self.gas_used = Column(bignum())
        self.gas_limit = Column(bignum())
        self.size = Column(pyarrow.int32())
        self.timestamp = Column(pyarrow.timestamp('s'))
        self.extra_data = Column(pyarrow.string())
        self.difficulty = Column(bignum())
        self.total_difficulty = Column(bignum())
        self.mix_hash = Column(pyarrow.string())
        self.base_fee_per_gas = Column(bignum())

    def append(self, block: BlockHeader):
        self.number.append(block['number'])
        self.hash.append(block['hash'])
        self.parent_hash.append(block['parentHash'])
        self.nonce.append(block['nonce'])
        self.sha3_uncles.append(block['sha3Uncles'])
        self.logs_bloom.append(block['logsBloom'])
        self.transactions_root.append(block['transactionsRoot'])
        self.state_root.append(block['stateRoot'])
        self.receipts_root.append(block['receiptsRoot'])
        self.miner.append(block['miner'])
        self.gas_used.append(block['gasUsed'])
        self.gas_limit.append(block['gasLimit'])
        self.size.append(block['size'])
        self.timestamp.append(block['timestamp'])
        self.extra_data.append(block['extraData'])
        self.difficulty.append(block['difficulty'])
        self.total_difficulty.append(block['totalDifficulty'])
        self.mix_hash.append(block['mixHash'])
        self.base_fee_per_gas.append(block['baseFeePerGas'])

    def to_table(self) -> pyarrow.Table:
        return pyarrow.table([
            self.number.build(),
            self.hash.build(),
            self.parent_hash.build(),
            self.nonce.build(),
            self.sha3_uncles.build(),
            self.logs_bloom.build(),
            self.transactions_root.build(),
            self.state_root.build(),
            self.receipts_root.build(),
            self.miner.build(),
            self.gas_used.build(),
            self.gas_limit.build(),
            self.size.build(),
            self.timestamp.build(),
            self.extra_data.build(),
            self.difficulty.build(),
            self.total_difficulty.build(),
            self.mix_hash.build(),
            self.base_fee_per_gas.build(),
        ], names=[
            'number',
            'hash',
            'parent_hash',
            'nonce',
            'sha3_uncles',
            'logs_bloom',
            'transactions_root',
            'state_root',
            'receipts_root',
            'miner',
            'gas_used',
            'gas_limit',
            'size',
            'timestamp',
            'extra_data',
            'difficulty',
            'total_difficulty',
            'mix_hash',
            'base_fee_per_gas',
        ])


class TxTableBuilder:
    def __init__(self):
        self.block_number = Column(pyarrow.int32())
        self.transaction_index = Column(pyarrow.int32())
        self.hash = Column(pyarrow.string())
        self.gas_price = Column(bignum())
        self.gas = Column(bignum())
        self.max_fee_per_gas = Column(bignum())
        self.max_priority_fee_per_gas = Column(bignum())
        self.from_ = Column(pyarrow.string())
        self.to = Column(pyarrow.string())
        self.sighash = Column(pyarrow.string())
        self.input = Column(pyarrow.string())
        self.nonce = Column(pyarrow.int64())
        self.value = Column(bignum())
        self.type = Column(pyarrow.int32())
        self.v = Column(pyarrow.string())
        self.r = Column(pyarrow.string())
        self.s = Column(pyarrow.string())
        self.y_parity = Column(pyarrow.int32())
        self.chain_id = Column(pyarrow.int32())
        self.access_list = Column(pyarrow.list_(
            pyarrow.struct([
                pyarrow.field('address', pyarrow.string(), nullable=False),
                pyarrow.field('storage_keys', pyarrow.list_(pyarrow.string()), nullable=False),
            ])
        ))
        self._id = Column(pyarrow.int64())

    def append(self, tx: Transaction):
        self.block_number.append(tx['blockNumber'])
        self.transaction_index.append(tx['transactionIndex'])
        self.hash.append(tx['hash'])
        self.gas_price.append(tx['gasPrice'])
        self.gas.append(tx['gas'])
        self.max_fee_per_gas.append(tx['maxFeePerGas'])
        self.max_priority_fee_per_gas.append(tx['maxPriorityFeePerGas'])
        self.from_.append(tx['from'])
        self.to.append(tx.get('to'))
        self.sighash.append(tx.get('sighash'))
        self.input.append(tx['input'])
        self.nonce.append(tx['nonce'])
        self.value.append(tx['value'])
        self.type.append(tx['type'])
        self.v.append(tx['v'])
        self.r.append(tx['r'])
        self.s.append(tx['s'])
        self.y_parity.append(tx['yParity'])
        self.chain_id.append(tx['chainId'])
        self.access_list.append(access_list(tx))
        self._id.append((tx['blockNumber'] << 24) + tx['transactionIndex'])

    def to_table(self):
        return pyarrow.table([
            self.block_number.build(),
            self.transaction_index.build(),
            self.hash.build(),
            self.gas_price.build(),
            self.gas.build(),
            self.max_fee_per_gas.build(),
            self.max_priority_fee_per_gas.build(),
            self.from_.build(),
            self.to.build(),
            self.sighash.build(),
            self.input.build(),
            self.nonce.build(),
            self.value.build(),
            self.type.build(),
            self.v.build(),
            self.r.build(),
            self.s.build(),
            self.y_parity.build(),
            self.chain_id.build(),
            self.access_list.build(),
            self._id.build(),
        ], names=[
            'block_number',
            'transaction_index',
            'hash',
            'gas_price',
            'gas',
            'max_fee_per_gas',
            'max_priority_fee_per_gas',
            'from',
            'to',
            'sighash',
            'input',
            'nonce',
            'value',
            'type',
            'v',
            'r',
            's',
            'y_parity',
            'chain_id',
            'access_list',
            '_id'
        ])


class LogTableBuilder:
    def __init__(self):
        self.block_number = Column(pyarrow.int32())
        self.log_index = Column(pyarrow.int32())
        self.transaction_index = Column(pyarrow.int32())
        self.address = Column(pyarrow.string())
        self.data = Column(pyarrow.string())
        self.topic0 = Column(pyarrow.string())
        self.topic1 = Column(pyarrow.string())
        self.topic2 = Column(pyarrow.string())
        self.topic3 = Column(pyarrow.string())
        self.removed = Column(pyarrow.bool_())

    def append(self, log: Log):
        self.block_number.append(log['blockNumber'])
        self.log_index.append(log['logIndex'])
        self.transaction_index.append(log['transactionIndex'])
        self.address.append(log['address'])
        self.data.append(log.get('data'))
        self.topic0.append(log.get('topic0'))
        self.topic1.append(log.get('topic1'))
        self.topic2.append(log.get('topic2'))
        self.topic3.append(log.get('topic3'))
        self.removed.append(log['removed'])

    def to_table(self):
        return pyarrow.table([
            self.block_number.build(),
            self.log_index.build(),
            self.transaction_index.build(),
            self.address.build(),
            self.data.build(),
            self.topic0.build(),
            self.topic1.build(),
            self.topic2.build(),
            self.topic3.build(),
            self.removed.build(),
        ], names=[
            'block_number',
            'log_index',
            'transaction_index',
            'address',
            'data',
            'topic0',
            'topic1',
            'topic2',
            'topic3',
            'removed',
        ])
