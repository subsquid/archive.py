import json
from typing import Any

import pyarrow

from sqa.fs import Fs
from sqa.writer.parquet import TableBuilder, Column, BaseParquetWriter, add_size_column, add_index_column
from .model import Block, BlockHeader, Transaction, Log, InternalTransaction


def JSON():
    return pyarrow.string()


def _to_json(val: Any) -> str | None:
    if val is None:
        return None
    else:
        return json.dumps(val)


def _to_sighash(data: str | None) -> str | None:
    if data is None:
        return None
    else:
        return data[:8] if len(data) >= 8 else None


def _to_int(val: str | None) -> int | None:
    return None if val is None else int(val)


class BlockTable(TableBuilder):
    def __init__(self):
        self.number = Column(pyarrow.int32())
        self.hash = Column(pyarrow.string())
        self.parent_hash = Column(pyarrow.string())
        self.tx_trie_root = Column(pyarrow.string())
        self.version = Column(pyarrow.int32())
        self.timestamp = Column(pyarrow.timestamp('ms', tz='UTC'))
        self.witness_address = Column(pyarrow.string())
        self.witness_signature = Column(pyarrow.string())

    def append(self, header: BlockHeader) -> None:
        self.number.append(header['height'])
        self.hash.append(header['hash'])
        self.parent_hash.append(header['parentHash'])
        self.tx_trie_root.append(header['txTrieRoot'])
        self.version.append(header.get('version'))
        self.timestamp.append(header['timestamp'])
        self.witness_address.append(header['witnessAddress'])
        self.witness_signature.append(header.get('witnessSignature'))


class TransactionTable(TableBuilder):
    def __init__(self):
        self.block_number = Column(pyarrow.int32())
        self.transaction_index = Column(pyarrow.int32())
        self.hash = Column(pyarrow.string())
        self.ret = Column(JSON())
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
        self.cancel_unfreeze_v2_amount = Column(JSON())

        # receipt
        self.result = Column(pyarrow.string())
        self.energy_fee = Column(pyarrow.int64())
        self.energy_usage = Column(pyarrow.int64())
        self.energy_usage_total = Column(pyarrow.int64())
        self.net_usage = Column(pyarrow.int64())
        self.net_fee = Column(pyarrow.int64())
        self.origin_energy_usage = Column(pyarrow.int64())
        self.energy_penalty_total = Column(pyarrow.int64())

        # TransferContract
        self._transfer_contract_owner = Column(pyarrow.string())
        self._transfer_contract_to = Column(pyarrow.string())

        # TransferAssetContract
        self._transfer_asset_contract_owner = Column(pyarrow.string())
        self._transfer_asset_contract_to = Column(pyarrow.string())
        self._transfer_asset_contract_asset = Column(pyarrow.string())

        # TriggerSmartContract
        self._trigger_smart_contract_owner = Column(pyarrow.string())
        self._trigger_smart_contract_contract = Column(pyarrow.string())
        self._trigger_smart_contract_sighash = Column(pyarrow.string())

    def append(self, block_number: int, tx: Transaction) -> None:
        self.block_number.append(block_number)
        self.transaction_index.append(tx['transactionIndex'])
        self.hash.append(tx['hash'])
        self.ret.append(_to_json(tx.get('ret')))
        self.signature.append(tx.get('signature'))
        self.type.append(tx['type'])
        self.parameter.append(_to_json(tx['parameter']))
        self.permission_id.append(tx.get('permissionId'))
        self.ref_block_bytes.append(tx.get('refBlockBytes'))
        self.ref_block_hash.append(tx.get('refBlockHash'))
        self.fee_limit.append(_to_int(tx.get('feeLimit')))
        self.expiration.append(tx.get('expiration'))
        self.timestamp.append(_to_int(tx.get('timestamp')))
        self.raw_data_hex.append(tx['rawDataHex'])

        self.fee.append(_to_int(tx.get('fee')))
        self.contract_result.append(tx.get('contractResult'))
        self.contract_address.append(tx.get('contractAddress'))
        self.res_message.append(tx.get('resMessage'))
        self.withdraw_amount.append(_to_int(tx.get('withdrawAmount')))
        self.unfreeze_amount.append(_to_int(tx.get('unfreezeAmount')))
        self.withdraw_expire_amount.append(_to_int(tx.get('withdrawExpireAmount')))
        self.cancel_unfreeze_v2_amount.append(_to_json(tx.get('cancelUnfreezeV2Amount')))

        self.result.append(tx.get('result'))
        self.energy_fee.append(_to_int(tx.get('energyFee')))
        self.energy_usage.append(_to_int(tx.get('energyUsage')))
        self.energy_usage_total.append(_to_int(tx.get('energyUsageTotal')))
        self.net_usage.append(_to_int(tx.get('netUsage')))
        self.net_fee.append(_to_int(tx.get('netFee')))
        self.origin_energy_usage.append(_to_int(tx.get('originEnergyUsage')))
        self.energy_penalty_total.append(_to_int(tx.get('energyPenaltyTotal')))

        if tx['type'] == 'TransferContract':
            self._transfer_contract_owner.append(tx['parameter']['value']['owner_address'])
            self._transfer_contract_to.append(tx['parameter']['value']['to_address'])
        else:
            self._transfer_contract_owner.append(None)
            self._transfer_contract_to.append(None)

        if tx['type'] == 'TransferAssetContract':
            self._transfer_asset_contract_owner.append(tx['parameter']['value']['owner_address'])
            self._transfer_asset_contract_to.append(tx['parameter']['value']['to_address'])
            self._transfer_asset_contract_asset.append(tx['parameter']['value']['asset_name'])
        else:
            self._transfer_asset_contract_owner.append(None)
            self._transfer_asset_contract_to.append(None)
            self._transfer_asset_contract_asset.append(None)

        if tx['type'] == 'TriggerSmartContract':
            self._trigger_smart_contract_owner.append(tx['parameter']['value']['owner_address'])
            self._trigger_smart_contract_contract.append(tx['parameter']['value']['contract_address'])
            self._trigger_smart_contract_sighash.append(_to_sighash(tx['parameter']['value'].get('data')))
        else:
            self._trigger_smart_contract_owner.append(None)
            self._trigger_smart_contract_contract.append(None)
            self._trigger_smart_contract_sighash.append(None)


class LogTable(TableBuilder):
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

    def append(self, block_number: int, log: Log):
        self.block_number.append(block_number)
        self.log_index.append(log['logIndex'])
        self.transaction_index.append(log['transactionIndex'])
        self.address.append(log['address'])
        self.data.append(log.get('data'))
        topics = iter(log.get('topics', []))
        self.topic0.append(next(topics, None))
        self.topic1.append(next(topics, None))
        self.topic2.append(next(topics, None))
        self.topic3.append(next(topics, None))


class InternalTransactionTable(TableBuilder):
    def __init__(self):
        self.block_number = Column(pyarrow.int32())
        self.transaction_index = Column(pyarrow.int32())
        self.internal_transaction_index = Column(pyarrow.int32())
        self.hash = Column(pyarrow.string())
        self.caller_address = Column(pyarrow.string())
        self.transer_to_address = Column(pyarrow.string())
        self.call_value_info = Column(JSON())
        self.note = Column(pyarrow.string())
        self.rejected = Column(pyarrow.bool_())
        self.extra = Column(JSON())

    def append(self, block_number: int, internal_tx: InternalTransaction):
        self.block_number.append(block_number)
        self.transaction_index.append(internal_tx['transactionIndex'])
        self.internal_transaction_index.append(internal_tx['internalTransactionIndex'])
        self.hash.append(internal_tx['hash'])
        self.caller_address.append(internal_tx['callerAddress'])
        self.transer_to_address.append(internal_tx.get('transferToAddress'))
        self.call_value_info.append(_to_json(internal_tx['callValueInfo']))
        self.note.append(internal_tx['note'])
        self.rejected.append(internal_tx.get('rejected'))
        self.extra.append(internal_tx.get('extra'))


class ParquetWriter(BaseParquetWriter):
    def __init__(self):
        self.blocks = BlockTable()
        self.transactions = TransactionTable()
        self.logs = LogTable()
        self.internal_transactions = InternalTransactionTable()

    def push(self, block: Block) -> None:
        self.blocks.append(block['header'])
        for log in block['logs']:
            self.logs.append(block['header']['height'], log)
        for tx in block['transactions']:
            self.transactions.append(block['header']['height'], tx)
        for internal_tx in block['internalTransactions']:
            self.internal_transactions.append(block['header']['height'], internal_tx)

    def _write(self, fs: Fs, tables: dict[str, pyarrow.Table]) -> None:
        write_parquet(fs, tables)

    def get_block_height(self, block: Block) -> int:
        return block['header']['height']

    def get_block_hash(self, block: Block) -> str:
        return '0x' + block['header']['hash'][16:]

    def get_block_parent_hash(self, block: Block) -> str:
        return '0x' + block['header']['parentHash'][16:]


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
        ('topic0', 'ascending'),
        ('address', 'ascending'),
        ('block_number', 'ascending'),
        ('log_index', 'ascending')
    ])
    logs = add_size_column(logs, 'data')
    logs = add_index_column(logs)

    fs.write_parquet(
        'logs.parquet',
        logs,
        row_group_size=10_000,
        use_dictionary=['address', 'topic0'],
        write_statistics=['_idx', 'address', 'topic0', 'block_number', 'transaction_index', 'log_index'],
        **kwargs
    )

    transactions = tables['transactions']
    transactions = transactions.sort_by([
        ('type', 'ascending'),
        ('_trigger_smart_contract_sighash', 'ascending'),
        ('_trigger_smart_contract_contract', 'ascending'),
        ('_trigger_smart_contract_owner', 'ascending'),
        ('_transfer_contract_owner', 'ascending'),
        ('_transfer_contract_to', 'ascending'),
        ('_transfer_asset_contract_owner', 'ascending'),
        ('_transfer_asset_contract_to', 'ascending'),
        ('_transfer_asset_contract_asset', 'ascending'),
        ('block_number', 'ascending'),
        ('transaction_index', 'ascending')
    ])
    transactions = add_size_column(transactions, 'raw_data_hex')
    transactions = add_index_column(transactions)

    fs.write_parquet(
        'transactions.parquet',
        transactions,
        row_group_size=10_000,
        use_dictionary=['type', 'ret'],
        write_statistics=[
            '_idx',
            'block_number',
            'transaction_index',
            'type',
            '_transfer_contract_owner',
            '_transfer_contract_to',
            '_transfer_asset_contract_owner',
            '_transfer_asset_contract_to',
            '_transfer_asset_contract_asset',
            '_trigger_smart_contract_owner',
            '_trigger_smart_contract_contract',
            '_trigger_smart_contract_sighash',
        ],
        **kwargs
    )

    internal_transactions = tables['internal_transactions']
    internal_transactions = internal_transactions.sort_by([
        ('transer_to_address', 'ascending'),
        ('caller_address', 'ascending'),
        ('block_number', 'ascending'),
        ('internal_transaction_index', 'ascending')
    ])
    internal_transactions = add_index_column(internal_transactions)

    fs.write_parquet(
        'internal_transactions.parquet',
        internal_transactions,
        use_dictionary=False,
        write_statistics=[
            '_idx',
            'block_number',
            'transaction_index',
            'internal_transaction_index',
            'transer_to_address',
            'caller_address'
        ],
        **kwargs
    )

    blocks = tables['blocks']

    fs.write_parquet(
        'blocks.parquet',
        blocks,
        write_statistics=['number'],
        **kwargs
    )
