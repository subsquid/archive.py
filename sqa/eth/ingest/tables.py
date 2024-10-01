from typing import Iterable, Literal

import pyarrow

from sqa.eth.ingest.model import Transaction, Log, Block, Qty, DebugFrame, DebugStateDiff, Address20, \
    Bytes, DebugStateMap, \
    TraceStateDiff, TraceDiff, TraceRec
from sqa.eth.ingest.util import qty2int, get_tx_status_from_traces
from sqa.writer.parquet import Column, TableBuilder


def qty():
    return pyarrow.string()


def bigfloat():
    return pyarrow.string()


class BlockTableBuilder(TableBuilder):
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
        self.l1_block_number = Column(pyarrow.int32())
        self.blob_gas_used = Column(qty())
        self.excess_blob_gas = Column(qty())

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
        self.l1_block_number.append(_qty2int(block.get('l1BlockNumber')))
        self.blob_gas_used.append(_qty2int(block.get('blobGasUsed')))
        self.excess_blob_gas.append(_qty2int(block.get('excessBlobGas')))


class TxTableBuilder(TableBuilder):
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
        self.chain_id = Column(pyarrow.uint64())
        self.sighash = Column(pyarrow.string())
        self.gas_used = Column(qty())
        self.cumulative_gas_used = Column(qty())
        self.effective_gas_price = Column(qty())
        self.contract_address = Column(pyarrow.string())
        self.type = Column(pyarrow.uint8())
        self.status = Column(pyarrow.int8())
        self.max_fee_per_blob_gas = Column(qty())
        self.blob_versioned_hashes = Column(pyarrow.list_(pyarrow.string()))
        self.l1_fee = Column(qty())
        self.l1_fee_scalar = Column(bigfloat())
        self.l1_gas_price = Column(qty())
        self.l1_gas_used = Column(qty())
        self.l1_blob_base_fee = Column(qty())
        self.l1_blob_base_fee_scalar = Column(pyarrow.uint32())
        self.l1_base_fee_scalar = Column(pyarrow.uint32())

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
        self.y_parity.append(_qty2int(tx.get('yParity')))
        self.chain_id.append(_qty2int(tx.get('chainId')))
        self.max_fee_per_blob_gas.append(_qty2int(tx.get('maxFeePerBlobGas')))
        self.blob_versioned_hashes.append(tx.get('blobVersionedHashes'))

        self.sighash.append(_to_sighash(tx_input))

        receipt = tx.get('receipt_')
        if receipt:
            self.gas_used.append(receipt['gasUsed'])
            self.cumulative_gas_used.append(receipt['cumulativeGasUsed'])
            self.effective_gas_price.append(receipt.get('effectiveGasPrice'))
            self.type.append(_qty2int(receipt.get('type')))
            self.status.append(qty2int(receipt['status']))
            self.contract_address.append(receipt.get('contractAddress'))
            self.l1_fee.append(receipt.get('l1Fee'))
            self.l1_fee_scalar.append(receipt.get('l1FeeScalar'))
            self.l1_gas_price.append(receipt.get('l1GasPrice'))
            self.l1_gas_used.append(receipt.get('l1GasUsed'))
            self.l1_blob_base_fee.append(receipt.get('l1BlobBaseFee'))
            self.l1_blob_base_fee_scalar.append(_qty2int(receipt.get('l1BlobBaseFeeScalar')))
            self.l1_base_fee_scalar.append(_qty2int(receipt.get('l1BaseFeeScalar')))
        else:
            self.gas_used.append(None)
            self.cumulative_gas_used.append(None)
            self.effective_gas_price.append(None)
            self.type.append(None)
            s = get_tx_status_from_traces(tx)
            if s:
                self.status.append(qty2int(s['status']))
                self.contract_address.append(s.get('contractAddress'))
            else:
                self.status.append(None)
                self.contract_address.append(None)
            self.l1_fee.append(None)
            self.l1_fee_scalar.append(None)
            self.l1_gas_price.append(None)
            self.l1_gas_used.append(None)
            self.l1_blob_base_fee.append(None)
            self.l1_blob_base_fee_scalar.append(None)
            self.l1_base_fee_scalar.append(None)


class LogTableBuilder(TableBuilder):
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
        self.address.append(log['address'].lower())
        self.data.append(log['data'])
        topics = iter(log['topics'])
        self.topic0.append(next(topics, None))
        self.topic1.append(next(topics, None))
        self.topic2.append(next(topics, None))
        self.topic3.append(next(topics, None))


class TraceTableBuilder(TableBuilder):
    def __init__(self):
        self.block_number = Column(pyarrow.int32())
        self.transaction_index = Column(pyarrow.int32())
        self.trace_address = Column(pyarrow.list_(pyarrow.int32()))
        self.subtraces = Column(pyarrow.int32())
        self.type = Column(pyarrow.string())
        self.error = Column(pyarrow.string())
        self.revert_reason = Column(pyarrow.string())

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
        self.call_sighash = Column(pyarrow.string())
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

    def trace_append(self, block_number: Qty, transaction_index: Qty, records: list[TraceRec]):
        bn = qty2int(block_number)
        tix = qty2int(transaction_index)
        for rec in records:
            self.block_number.append(bn)
            self.trace_address.append(rec['traceAddress'])
            self.subtraces.append(rec['subtraces'])
            self.transaction_index.append(tix)
            self.type.append(rec['type'])
            self.error.append(rec.get('error'))
            self.revert_reason.append(None)

            if rec['type'] == 'create':
                action = rec['action']
                self.create_from.append(action['from'])
                self.create_value.append(action['value'])
                self.create_gas.append(action['gas'])
                self.create_init.append(action.get('init'))
                if result := rec.get('result'):
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

            if rec['type'] == 'call':
                action = rec['action']
                self.call_from.append(action['from'])
                self.call_to.append(action['to'])
                self.call_value.append(action['value'])
                self.call_gas.append(action['gas'])
                self.call_sighash.append(_to_sighash(action['input']))
                self.call_input.append(action['input'])
                self.call_type.append(action['callType'])
                if result := rec.get('result'):
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
                self.call_sighash.append(None)
                self.call_input.append(None)
                self.call_type.append(None)
                self.call_result_gas_used.append(None)
                self.call_result_output.append(None)

            if rec['type'] == 'suicide':
                action = rec['action']
                self.suicide_address.append(action['address'])
                self.suicide_refund_address.append(action['refundAddress'])
                self.suicide_balance.append(action['balance'])
            else:
                self.suicide_address.append(None)
                self.suicide_refund_address.append(None)
                self.suicide_balance.append(None)

            if rec['type'] == 'reward':
                action = rec['action']
                self.reward_author.append(action['author'])
                self.reward_value.append(action['value'])
                self.reward_type.append(action['rewardType'])
            else:
                self.reward_author.append(None)
                self.reward_value.append(None)
                self.reward_type.append(None)

    def debug_append(self, block_number: Qty, transaction_index: Qty, top: DebugFrame):
        bn = qty2int(block_number)
        tix = qty2int(transaction_index)
        for addr, subtraces, frame in _traverse_frame(top, []):
            trace_type: Literal['create', 'call', 'suicide']
            frame_type = frame['type']
            if frame_type in ('CALL', 'CALLCODE', 'STATICCALL', 'DELEGATECALL', 'INVALID', 'Call'):
                trace_type = 'call'
            elif frame_type in ('CREATE', 'CREATE2', 'Create'):
                trace_type = 'create'
            elif frame_type == 'SELFDESTRUCT':
                trace_type = 'suicide'
            elif frame_type == 'STOP':
                assert top['type'] == 'STOP'
                return
            else:
                raise Exception(f'Unknown frame type - {frame_type}')

            self.block_number.append(bn)
            self.transaction_index.append(tix)
            self.trace_address.append(addr)
            self.subtraces.append(subtraces)
            self.error.append(frame.get('error'))
            self.revert_reason.append(frame.get('revertReason'))
            self.type.append(trace_type)

            if trace_type == 'create':
                self.create_from.append(frame['from'])
                self.create_value.append(frame['value'])
                self.create_gas.append(frame.get('gas'))
                self.create_init.append(frame['input'])
                self.create_result_gas_used.append(frame.get('gasUsed'))
                self.create_result_code.append(frame.get('output'))
                self.create_result_address.append(frame.get('to'))
            else:
                self.create_from.append(None)
                self.create_value.append(None)
                self.create_gas.append(None)
                self.create_init.append(None)
                self.create_result_gas_used.append(None)
                self.create_result_code.append(None)
                self.create_result_address.append(None)

            if trace_type == 'call':
                self.call_from.append(frame['from'])
                self.call_to.append(frame['to'])
                self.call_value.append(frame.get('value'))
                self.call_gas.append(frame.get('gas'))
                self.call_sighash.append(_to_sighash(frame['input']))
                self.call_input.append(frame['input'])
                self.call_type.append(frame_type.lower())
                self.call_result_gas_used.append(frame.get('gasUsed'))
                self.call_result_output.append(frame.get('output'))
            else:
                self.call_from.append(None)
                self.call_to.append(None)
                self.call_value.append(None)
                self.call_gas.append(None)
                self.call_sighash.append(None)
                self.call_input.append(None)
                self.call_type.append(None)
                self.call_result_gas_used.append(None)
                self.call_result_output.append(None)

            if trace_type == 'suicide':
                self.suicide_address.append(frame.get('from'))
                self.suicide_refund_address.append(frame.get('to'))
                self.suicide_balance.append(frame.get('value'))
            else:
                self.suicide_address.append(None)
                self.suicide_refund_address.append(None)
                self.suicide_balance.append(None)

            self.reward_author.append(None)
            self.reward_value.append(None)
            self.reward_type.append(None)


def _traverse_frame(frame: DebugFrame, address: list[int]) -> Iterable[tuple[list[int], int, DebugFrame]]:
    subcalls = frame.get('calls', ())
    yield address, len(subcalls), frame
    for i, call in enumerate(subcalls):
        yield from _traverse_frame(call, [*address, i])


class StateDiffTableBuilder(TableBuilder):
    def __init__(self):
        self.block_number = Column(pyarrow.int32())
        self.transaction_index = Column(pyarrow.int32())
        self.address = Column(pyarrow.string())
        self.key = Column(pyarrow.string())  # 'balance', 'code', 'nonce', '0x00000023420387'
        self.kind = Column(pyarrow.string())  # +, *, - (DIC)
        self.prev = Column(pyarrow.string())
        self.next = Column(pyarrow.string())

    def debug_append(
        self,
        block_number: Qty,
        transaction_index: Qty,
        diff: DebugStateDiff
    ):
        bn = qty2int(block_number)
        tix = qty2int(transaction_index)
        pre = diff['pre']
        post = diff['post']

        for address, pre_map in pre.items():
            post_map = post.get(address, {})
            self._debug_append_address(bn, tix, address, pre_map, post_map)

        for address, post_map in post.items():
            if address in pre:
                pass
            else:
                self._debug_append_address(bn, tix, address, {}, post_map)

    def _debug_append_address(
        self,
        block_number: int,
        transaction_index: int,
        address: Address20,
        pre: DebugStateMap,
        post: DebugStateMap
    ):
        for key, prev, nxt in _known_keys_diff(pre, post):
            self._debug_append_row(block_number, transaction_index, address, key, prev, nxt)

        pre_storage = pre.get('storage', {})
        post_storage = post.get('storage', {})

        for key, prev in pre_storage.items():
            nxt = post_storage.get(key)
            self._debug_append_row(block_number, transaction_index, address, key, prev, nxt)

        for key, nxt in post_storage.items():
            if key in pre_storage:
                pass
            else:
                self._debug_append_row(block_number, transaction_index, address, key, None, nxt)

    def _debug_append_row(
        self,
        block_number: int,
        transaction_index: int,
        address: Address20,
        key: str,
        pre: Bytes | None,
        post: Bytes | None
    ):
        self.block_number.append(block_number)
        self.transaction_index.append(transaction_index)
        self.address.append(address)
        self.key.append(key)

        kind: str
        if pre is None:
            assert post is not None
            kind = '+'
        elif post is None:
            kind = '-'
        else:
            kind = '*'

        self.kind.append(kind)
        self.prev.append(pre)
        self.next.append(post)

    def trace_append(
        self,
        block_number: Qty,
        transaction_index: Qty,
        diff: dict[Address20, TraceStateDiff]
    ):
        bn = qty2int(block_number)
        tix = qty2int(transaction_index)
        for address, diff in diff.items():
            self._trace_append_diff(
                bn,
                tix,
                address,
                diff
            )

    def _trace_append_diff(
        self,
        block_number: int,
        transaction_index: int,
        address: Address20,
        diff: TraceStateDiff
    ):
        self._trace_append_row(block_number, transaction_index, address, 'balance', diff['balance'])
        self._trace_append_row(block_number, transaction_index, address, 'code', diff['code'])
        self._trace_append_row(block_number, transaction_index, address, 'nonce', diff['nonce'])
        for slot, d in diff['storage'].items():
            self._trace_append_row(block_number, transaction_index, address, slot, d)

    def _trace_append_row(
            self,
            block_number: int,
            transaction_index: int,
            address: Address20,
            key: str,
            diff: TraceDiff
    ):
        if diff == '=':
            return

        self.block_number.append(block_number)
        self.transaction_index.append(transaction_index)
        self.address.append(address)
        self.key.append(key)

        if '+' in diff:
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


def _known_keys_diff(pre: DebugStateMap, post: DebugStateMap) -> Iterable[tuple[str, Bytes | None, Bytes | None]]:
    for key in ('balance', 'code', 'nonce'):
        prev = pre.get(key)
        nxt = post.get(key)
        if key == 'nonce':
            prev = None if prev is None else hex(prev)
            nxt = None if nxt is None else hex(nxt)
        if nxt is None:
            pass
        else:
            yield key, prev, nxt


def _to_sighash(tx_input: str) -> str | None:
    if len(tx_input) >= 10:
        return tx_input[:10]
    else:
        return None


def _qty2int(v: Qty | None) -> int | None:
    return None if v is None else qty2int(v)
