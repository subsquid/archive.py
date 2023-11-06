import json
from typing import Any

import pyarrow

from sqa.fs import Fs
from sqa.writer.parquet import TableBuilder, Column, BaseParquetSink, add_size_column, add_index_column
from .model import Block, Transaction, Log, InternalTransaction, BlockData


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


class BlockTable(TableBuilder):
    def __init__(self):
        self.number = Column(pyarrow.int32())
        self.hash = Column(binary())
        self.parent_hash = Column(binary())
        self.tx_trie_root = Column(pyarrow.string())
        self.version = Column(pyarrow.int32())
        self.timestamp = Column(pyarrow.timestamp('ms', tz='UTC'))
        self.witness_address = Column(pyarrow.string())
        self.witness_signature = Column(pyarrow.string())

    def append(self, block: Block) -> None:
        self.number.append(block['block_header']['raw_data'].get('number', 0))
        self.hash.append(block['blockID'])
        self.parent_hash.append(block['block_header']['raw_data']['parentHash'])
        self.tx_trie_root.append(block['block_header']['raw_data']['txTrieRoot'])
        self.version.append(block['block_header']['raw_data']['version'])
        self.timestamp.append(block['block_header']['raw_data']['timestamp'])
        self.witness_address.append(block['block_header']['raw_data']['witness_address'])
        self.witness_signature.append(block['block_header']['witness_signature'])


class TransactionTable(TableBuilder):
    def __init__(self):
        self.block_number = Column(pyarrow.int32())
        self.hash = Column(binary())
        self.ret = Column(pyarrow.string())
        self.signature = Column(pyarrow.list_(pyarrow.string()))
        self.type = Column(pyarrow.string())
        self.parameter = Column(JSON())
        self.permission_id = Column(pyarrow.int32())
        self.ref_block_bytes = Column(pyarrow.string())
        self.ref_block_hash = Column(pyarrow.string())
        self.fee_limit = Column(pyarrow.int64())
        self.expiration = Column(pyarrow.timestamp('ms', tz='UTC'))
        self.timestamp = Column(pyarrow.timestamp('ms', tz='UTC'))
        self.raw_data_hex = Column(pyarrow.string())

        # info
        self.fee = Column(pyarrow.int64())
        self.contract_result = Column(pyarrow.string())
        self.contract_address = Column(pyarrow.string())
        self.res_message = Column(pyarrow.string())
        self.withdraw_amount = Column(pyarrow.int64())
        self.unfreeze_amount = Column(pyarrow.int64())
        self.withdraw_expire_amount = Column(pyarrow.int64())
        self.cancel_unfreezeV2_amount = Column(JSON())

        # receipt
        self.result = Column(pyarrow.string())
        self.energy_fee = Column(pyarrow.int64())
        self.energy_usage = Column(pyarrow.int64())
        self.energy_usage_total = Column(pyarrow.int64())
        self.net_usage = Column(pyarrow.int64())
        self.net_fee = Column(pyarrow.int64())
        self.origin_energy_usage = Column(pyarrow.int64())
        self.energy_penalty_total = Column(pyarrow.int64())

    def append(self, block_number: int, tx: Transaction) -> None:
        self.block_number.append(block_number)
        self.hash.append(tx['txID'])
        assert len(tx['ret']) == 1
        self.ret.append(tx['ret'][0]['contractRet'])
        self.signature.append(tx['signature'])
        assert len(tx['raw_data']['contract']) == 1
        contract = tx['raw_data']['contract'][0]
        self.type.append(contract['type'])
        self.parameter.append(_to_json(contract['parameter']))
        self.permission_id.append(contract.get('Permission_id'))
        self.ref_block_bytes.append(tx['raw_data']['ref_block_bytes'])
        self.ref_block_hash.append(tx['raw_data']['ref_block_hash'])
        self.fee_limit.append(tx['raw_data'].get('fee_limit'))
        self.expiration.append(tx['raw_data']['expiration'])
        self.timestamp.append(tx['raw_data'].get('timestamp'))
        self.raw_data_hex.append(tx['raw_data_hex'])

        if info := tx.get('info'):
            self.fee.append(info.get('fee'))
            assert len(info['contractResult']) == 1
            self.contract_result.append(info['contractResult'][0])
            self.contract_address.append(info.get('contract_address'))
            self.res_message.append(info.get('resMessage'))
            self.withdraw_amount.append(info.get('withdraw_amount'))
            self.unfreeze_amount.append(info.get('unfreeze_amount'))
            self.withdraw_expire_amount.append(info.get('withdraw_expire_amount'))
            self.cancel_unfreezeV2_amount.append(_to_json(info.get('cancel_unfreezeV2_amount')))

            self.result.append(info['receipt'].get('result'))
            self.energy_fee.append(info['receipt'].get('energy_fee'))
            self.energy_usage.append(info['receipt'].get('energy_usage'))
            self.energy_usage_total.append(info['receipt'].get('energy_usage_total'))
            self.net_usage.append(info['receipt'].get('net_usage'))
            self.net_fee.append(info['receipt'].get('net_fee'))
            self.origin_energy_usage.append(info['receipt'].get('origin_energy_usage'))
            self.energy_penalty_total.append(info['receipt'].get('energy_penalty_total'))
        else:
            self.fee.append(None)
            self.contract_result.append(None)
            self.contract_address.append(None)
            self.res_message.append(None)
            self.withdraw_amount.append(None)
            self.unfreeze_amount.append(None)
            self.withdraw_expire_amount.append(None)
            self.cancel_unfreezeV2_amount.append(None)

            self.result.append(None)
            self.energy_fee.append(None)
            self.energy_usage.append(None)
            self.energy_usage_total.append(None)
            self.net_usage.append(None)
            self.net_fee.append(None)
            self.origin_energy_usage.append(None)
            self.energy_penalty_total.append(None)


class LogTable(TableBuilder):
    def __init__(self):
        self.block_number = Column(pyarrow.int32())
        self.transaction_hash = Column(pyarrow.string())
        self.address = Column(pyarrow.string())
        self.data = Column(pyarrow.string())
        self.topic0 = Column(pyarrow.string())
        self.topic1 = Column(pyarrow.string())
        self.topic2 = Column(pyarrow.string())
        self.topic3 = Column(pyarrow.string())

    def append(self, block_number: int, tx_hash: str, log: Log):
        self.block_number.append(block_number)
        self.transaction_hash.append(tx_hash)
        self.address.append(log['address'])
        self.data.append(log.get('data'))
        topics = iter(log['topics'])
        self.topic0.append(next(topics, None))
        self.topic1.append(next(topics, None))
        self.topic2.append(next(topics, None))
        self.topic3.append(next(topics, None))


class InternalTransactionTable(TableBuilder):
    def __init__(self):
        self.block_number = Column(pyarrow.int32())
        self.transaction_hash = Column(pyarrow.string())
        self.hash = Column(pyarrow.string())
        self.caller_address = Column(pyarrow.string())
        self.transer_to_address = Column(pyarrow.string())
        self.call_value_info = Column(JSON())
        self.note = Column(pyarrow.string())
        self.rejected = Column(pyarrow.bool_())
        self.extra = Column(JSON())

    def append(self, block_number: int, tx_hash: str, internal_tx: InternalTransaction):
        self.block_number.append(block_number)
        self.transaction_hash.append(tx_hash)
        self.hash.append(internal_tx['hash'])
        self.caller_address.append(internal_tx['caller_address'])
        self.transer_to_address.append(internal_tx['transferTo_address'])
        self.call_value_info.append(_to_json(internal_tx['callValueInfo']))
        self.note.append(internal_tx['note'])
        self.rejected.append(internal_tx.get('rejected'))
        self.extra.append(internal_tx.get('extra'))


class ParquetSink(BaseParquetSink):
    def __init__(self):
        self.blocks = BlockTable()
        self.transactions = TransactionTable()
        self.logs = LogTable()
        self.internal_transactions = InternalTransactionTable()

    def push(self, data: BlockData) -> None:
        self.blocks.append(data['block'])
        for tx in data['block'].get('transactions', []):
            self.transactions.append(data['height'], tx)
            if info := tx.get('info'):
                for log in info.get('log', []):
                    self.logs.append(data['height'], tx['txID'], log)
                for internal_tx in info.get('internal_transactions', []):
                    self.internal_transactions.append(data['height'], tx['txID'], internal_tx)

    def _write(self, fs: Fs, tables: dict[str, pyarrow.Table]) -> None:
        write_parquet(fs, tables)


def write_parquet(fs: Fs, tables: dict[str, pyarrow.Table]) -> None:
    kwargs = {
        'data_page_size': 128 * 1024,
        'dictionary_pagesize_limit': 128 * 1024,
        'compression': 'zstd',
        'write_page_index': True,
        'write_batch_size': 100
    }

    logs = tables['logs']
    logs = logs.sort_by([
        ('address', 'ascending'),
        ('topic0', 'ascending'),
        ('block_number', 'ascending'),
    ])
    logs = add_size_column(logs, 'data')
    logs = add_index_column(logs)

    fs.write_parquet(
        'logs.parquet',
        logs,
        row_group_size=10_000,
        use_dictionary=['address', 'topic0'],
        write_statistics=['_idx', 'address', 'topic0', 'block_number'],
        **kwargs
    )

    transactions = tables['transactions']
    transactions = transactions.sort_by([
        ('block_number', 'ascending'),
    ])
    transactions = add_size_column(transactions, 'raw_data_hex')
    transactions = add_index_column(transactions)

    fs.write_parquet(
        'transactions.parquet',
        transactions,
        row_group_size=10_000,
        use_dictionary=[],
        write_statistics=['_idx', 'block_number'],
        **kwargs
    )

    internal_transactions = tables['internal_transactions']
    internal_transactions = add_index_column(internal_transactions)

    fs.write_parquet(
        'internal_transactions.parquet',
        internal_transactions,
        use_dictionary=False,
        write_statistics=['_idx'],
        **kwargs
    )

    blocks = tables['blocks']

    fs.write_parquet(
        'blocks.parquet',
        blocks,
        use_dictionary=[],
        write_statistics=[],
        **kwargs
    )
