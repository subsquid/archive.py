import pyarrow

from etha.ingest.column import Column
from etha.ingest.model import Transaction, Log, Trace, Block, StateDiff, Address20, Qty, Diff
from etha.ingest.util import qty2int


def qty():
    return pyarrow.string()


class TableBuilderBase:
    def to_table(self) -> pyarrow.Table:
        arrays = []
        names = []
        for n, c in self.__dict__.items():
            if isinstance(c, Column):
                names.append(n)
                arrays.append(c.build())
        return pyarrow.table(arrays, names=names)

    def bytesize(self) -> int:
        size = 0
        for c in self.__dict__.values():
            if isinstance(c, Column):
                size += c.bytesize()
        return size


class BlockTableBuilder(TableBuilderBase):
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
        self.mix_hash = Column(pyarrow.string())
        self.miner = Column(pyarrow.string())
        self.difficulty = Column(qty())
        self.total_difficulty = Column(qty())
        self.extra_data = Column(pyarrow.string())
        self.size = Column(pyarrow.int32())
        self.gas_limit = Column(qty())
        self.gas_used = Column(qty())
        self.timestamp = Column(pyarrow.timestamp('s'))
        self.base_fee_per_gas = Column(qty())

    def append(self, block: Block) -> None:
        self.number.append(qty2int(block['number']))
        self.hash.append(block['hash'])
        self.parent_hash.append(block['parentHash'])
        self.nonce.append(block.get('nonce'))
        self.sha3_uncles.append(block['sha3Uncles'])
        self.logs_bloom.append(block['logsBloom'])
        self.transactions_root.append(block['transactionsRoot'])
        self.state_root.append(block['stateRoot'])
        self.receipts_root.append(block['receiptsRoot'])
        self.mix_hash.append(block.get('mixHash'))
        self.miner.append(block['miner'])
        self.difficulty.append(block.get('difficulty'))
        self.total_difficulty.append(block.get('totalDifficulty'))
        self.extra_data.append(block['extraData'])
        self.size.append(qty2int(block['size']))
        self.gas_used.append(block['gasUsed'])
        self.gas_limit.append(block['gasLimit'])
        self.timestamp.append(qty2int(block['timestamp']))
        self.base_fee_per_gas.append(block.get('baseFeePerGas'))


class TxTableBuilder(TableBuilderBase):
    def __init__(self):
        self.block_number = Column(pyarrow.int32())
        self.__dict__['from'] = Column(pyarrow.string())
        self.gas = Column(qty())
        self.gas_price = Column(qty())
        self.max_fee_per_gas = Column(qty())
        self.max_priority_fee_per_gas = Column(qty())
        self.hash = Column(pyarrow.string())
        self.input = Column(pyarrow.string())
        self.nonce = Column(pyarrow.int64())
        self.to = Column(pyarrow.string())
        self.transaction_index = Column(pyarrow.int32())
        self.value = Column(qty())
        self.v = Column(pyarrow.string())
        self.r = Column(pyarrow.string())
        self.s = Column(pyarrow.string())
        self.y_parity = Column(pyarrow.int8())
        self.chain_id = Column(pyarrow.int32())
        self.sighash = Column(pyarrow.string())
        self.gas_used = Column(qty())
        self.cumulative_gas_used = Column(qty())
        self.effective_gas_price = Column(qty())
        self.type = Column(pyarrow.int8())
        self.status = Column(pyarrow.int8())

    def append(self, tx: Transaction):
        block_number = qty2int(tx['blockNumber'])
        tx_index = qty2int(tx['transactionIndex'])
        tx_input = tx['input']

        self.block_number.append(block_number)
        self.__dict__['from'].append(tx['from'])
        self.gas.append(tx['gas'])
        self.gas_price.append(tx['gasPrice'])
        self.max_fee_per_gas.append(tx.get('maxFeePerGas'))
        self.max_priority_fee_per_gas.append(tx.get('maxPriorityFeePerGas'))
        self.hash.append(tx['hash'])
        self.input.append(tx_input)
        self.nonce.append(qty2int(tx['nonce']))
        self.to.append(tx.get('to'))
        self.transaction_index.append(tx_index)
        self.value.append(tx['value'])
        self.v.append(tx.get('v'))
        self.r.append(tx.get('r'))
        self.s.append(tx.get('s'))
        self.y_parity.append(tx.get('yParity') and qty2int(tx['yParity']))
        self.chain_id.append(tx.get('chainId') and qty2int(tx['chainId']))

        self.sighash.append(tx_input[:10] if len(tx_input) >= 10 else None)

        receipt = tx.get('receipt_')
        if receipt:
            self.gas_used.append(receipt['gasUsed'])
            self.cumulative_gas_used.append(receipt['cumulativeGasUsed'])
            self.effective_gas_price.append(receipt['effectiveGasPrice'])
            self.type.append(qty2int(receipt['type']))
            self.status.append(qty2int(receipt['status']))
        else:
            self.gas_used.append(None)
            self.cumulative_gas_used.append(None)
            self.effective_gas_price.append(None)
            self.type.append(None)
            self.status.append(None)


class LogTableBuilder(TableBuilderBase):
    def __init__(self):
        self.block_number = Column(pyarrow.int32())
        self.log_index = Column(pyarrow.int32())
        self.transaction_index = Column(pyarrow.int32())
        self.transaction_hash = Column(pyarrow.string())
        self.address = Column(pyarrow.string())
        self.data = Column(pyarrow.string())
        self.topic0 = Column(pyarrow.string())
        self.topic1 = Column(pyarrow.string())
        self.topic2 = Column(pyarrow.string())
        self.topic3 = Column(pyarrow.string())

    def append(self, log: Log):
        self.block_number.append(qty2int(log['blockNumber']))
        self.log_index.append(qty2int(log['logIndex']))
        self.transaction_index.append(qty2int(log['transactionIndex']))
        self.transaction_hash.append(log['transactionHash'])
        self.address.append(log['address'])
        self.data.append(log['data'])
        topics = iter(log['topics'])
        self.topic0.append(next(topics, None))
        self.topic1.append(next(topics, None))
        self.topic2.append(next(topics, None))
        self.topic3.append(next(topics, None))


class TraceTableBuilder(TableBuilderBase):
    def __init__(self):
        self.block_number = Column(pyarrow.int32())
        self.trace_address = Column(pyarrow.list_(pyarrow.int32()))
        self.subtraces = Column(pyarrow.int32())
        self.transaction_index = Column(pyarrow.int32())
        self.type = Column(pyarrow.string())
        self.error = Column(pyarrow.string())

        self.create_from = Column(pyarrow.string())
        self.create_value = Column(qty())
        self.create_gas = Column(qty())
        self.create_init = Column(pyarrow.string())
        self.create_result_gas_used = Column(qty())
        self.create_result_code = Column(pyarrow.string())
        self.create_result_address = Column(pyarrow.string())

        self.call_from = Column(pyarrow.string())
        self.call_to = Column(pyarrow.string())
        self.call_value = Column(qty())
        self.call_gas = Column(qty())
        self.call_input = Column(pyarrow.string())
        self.call_type = Column(pyarrow.string())
        self.call_result_gas_used = Column(qty())
        self.call_result_output = Column(pyarrow.string())

        self.suicide_address = Column(pyarrow.string())
        self.suicide_refund_address = Column(pyarrow.string())
        self.suicide_balance = Column(qty())

        self.reward_author = Column(pyarrow.string())
        self.reward_value = Column(qty())
        self.reward_type = Column(pyarrow.string())

    def append(self, block_number: Qty, transaction_index: Qty, trace: Trace):
        self.block_number.append(qty2int(block_number))
        self.trace_address.append(trace['traceAddress'])
        self.subtraces.append(trace['subtraces'])
        self.transaction_index.append(qty2int(transaction_index))
        self.type.append(trace['type'])
        self.error.append(trace.get('error'))

        if trace['type'] == 'create':
            action = trace['action']
            self.create_from.append(action['from'])
            self.create_value.append(action['value'])
            self.create_gas.append(action['gas'])
            self.create_init.append(action['init'])
            if result := trace.get('result'):
                self.create_result_gas_used.append(result['gasUsed'])
                self.create_result_code.append(result['code'])
                self.create_result_address.append(result['address'])
            else:
                self.create_result_gas_used.append(None)
                self.create_result_code.append(None)
                self.create_result_address.append(None)
        else:
            self.create_from.append(None)
            self.create_value.append(None)
            self.create_gas.append(None)
            self.create_init.append(None)
            self.create_result_gas_used.append(None)
            self.create_result_code.append(None)
            self.create_result_address.append(None)

        if trace['type'] == 'call':
            action = trace['action']
            self.call_from.append(action['from'])
            self.call_to.append(action['to'])
            self.call_value.append(action['value'])
            self.call_gas.append(action['gas'])
            self.call_input.append(action['input'])
            self.call_type.append(action['callType'])
            if result := trace.get('result'):
                self.call_result_gas_used.append(result['gasUsed'])
                self.call_result_output.append(result['output'])
            else:
                self.call_result_gas_used.append(None)
                self.call_result_output.append(None)
        else:
            self.call_from.append(None)
            self.call_to.append(None)
            self.call_value.append(None)
            self.call_gas.append(None)
            self.call_input.append(None)
            self.call_type.append(None)
            self.call_result_gas_used.append(None)
            self.call_result_output.append(None)

        if trace['type'] == 'suicide':
            action = trace['action']
            self.suicide_address.append(action['address'])
            self.suicide_refund_address.append(action['refundAddress'])
            self.suicide_balance.append(action['balance'])
        else:
            self.suicide_address.append(None)
            self.suicide_refund_address.append(None)
            self.suicide_balance.append(None)

        if trace['type'] == 'reward':
            action = trace['action']
            self.reward_author.append(action['author'])
            self.reward_value.append(action['value'])
            self.reward_type.append(action['rewardType'])
        else:
            self.reward_author.append(None)
            self.reward_value.append(None)
            self.reward_type.append(None)


class StateDiffTableBuilder(TableBuilderBase):
    def __init__(self):
        self.block_number = Column(pyarrow.int32())
        self.transaction_index = Column(pyarrow.int32())
        self.address = Column(pyarrow.string())
        self.key = Column(pyarrow.string())  # 'balance', 'code', 'nonce', '0x00000023420387'
        self.kind = Column(pyarrow.string())  # =, +, *, - (DIC)
        self.prev = Column(pyarrow.string())
        self.next = Column(pyarrow.string())

    def append(
        self,
        block_number: Qty,
        transaction_index: Qty,
        address: Address20,
        diff: StateDiff
    ):
        bn = qty2int(block_number)
        tix = qty2int(transaction_index)
        self._append(bn, tix, address, 'balance', diff['balance'])
        self._append(bn, tix, address, 'code', diff['code'])
        self._append(bn, tix, address, 'nonce', diff['nonce'])
        for slot, d in diff['storage'].items():
            self._append(bn, tix, address, slot, d)

    def _append(
        self,
        block_number: int,
        transaction_index: int,
        address: Address20,
        key: str,
        diff: Diff
    ):
        self.block_number.append(block_number)
        self.transaction_index.append(transaction_index)
        self.address.append(address)
        self.key.append(key)

        if diff == '=':
            self.kind.append('=')
            self.prev.append(None)
            self.next.append(None)
        elif '+' in diff:
            self.kind.append('+')
            self.prev.append(None)
            self.next.append(diff['+'])
        elif '*' in diff:
            self.kind.append('*')
            self.prev.append(diff['*']['from'])
            self.next.append(diff['*']['to'])
        elif '-' in diff:
            self.kind.append('-')
            self.prev.append(diff['-'])
            self.next.append(None)
        else:
            raise ValueError(f'unsupported state diff kind - {diff}')
