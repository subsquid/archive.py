import pyarrow

from sqa.fs import Fs
from sqa.writer.parquet import TableBuilder, Column, BaseParquetWriter, add_index_column, add_size_column
from .model import BlockHeader, Transaction, TransactionInput, TransactionOutput, Block, Receipt


class BlockTable(TableBuilder):
    def __init__(self):
        self.number = Column(pyarrow.int32())
        self.hash = Column(pyarrow.string())
        self.da_height = Column(pyarrow.int32())
        self.transactions_root = Column(pyarrow.string())
        self.transactions_count = Column(pyarrow.uint64())
        self.message_receipt_root = Column(pyarrow.string())
        self.message_receipt_count = Column(pyarrow.uint64())
        self.prev_root = Column(pyarrow.string())
        self.application_hash = Column(pyarrow.string())
        self.time = Column(pyarrow.uint64())

    def append(self, block: BlockHeader) -> None:
        self.number.append(block['height'])
        self.hash.append(block['hash'])
        self.da_height.append(int(block['daHeight']))
        self.transactions_root.append(block['transactionsRoot'])
        self.transactions_count.append(int(block['transactionsCount']))
        self.message_receipt_root.append(block['messageReceiptRoot'])
        self.message_receipt_count.append(int(block['messageReceiptCount']))
        self.prev_root.append(block['prevRoot'])
        self.application_hash.append(block['applicationHash'])
        self.time.append(int(block['time']))


class TransactionTable(TableBuilder):
    def __init__(self):
        self.block_number = Column(pyarrow.int32())
        self.transaction_index = Column(pyarrow.int32())
        self.hash = Column(pyarrow.string())
        self.input_asset_ids = Column(pyarrow.list_(pyarrow.string()))
        self.input_contracts = Column(pyarrow.list_(pyarrow.string()))
        self.gas_price = Column(pyarrow.uint64())
        self.script_gas_limit = Column(pyarrow.uint64())
        self.maturity = Column(pyarrow.uint32())
        self.mint_amount = Column(pyarrow.uint64())
        self.mint_asset_id = Column(pyarrow.string())
        self.tx_pointer = Column(pyarrow.string())
        self.is_script = Column(pyarrow.bool_())
        self.is_create = Column(pyarrow.bool_())
        self.is_mint = Column(pyarrow.bool_())
        self.type = Column(pyarrow.string())
        self.witnesses = Column(pyarrow.list_(pyarrow.string()))
        self.receipts_root = Column(pyarrow.string())
        self.script = Column(pyarrow.string())
        self.script_data = Column(pyarrow.string())
        self.bytecode_witness_index = Column(pyarrow.int32())
        self.bytecode_length = Column(pyarrow.uint64())
        self.salt = Column(pyarrow.string())
        self.storage_slots = Column(pyarrow.list_(pyarrow.string()))
        self.raw_payload = Column(pyarrow.string())
        # status
        self.status = Column(pyarrow.string())
        self.success_status_transaction_id = Column(pyarrow.string())
        self.success_status_time = Column(pyarrow.uint64())
        self.success_status_program_state_return_type = Column(pyarrow.string())
        self.success_status_program_state_data = Column(pyarrow.string())
        self.squeezed_out_status_reason = Column(pyarrow.string())
        self.failure_status_transaction_id = Column(pyarrow.string())
        self.failure_status_time = Column(pyarrow.uint64())
        self.failure_status_reason = Column(pyarrow.string())
        self.failure_status_program_state_return_type = Column(pyarrow.string())
        self.failure_status_program_state_data = Column(pyarrow.string())
        # input contract
        self.input_contract_utxo_id = Column(pyarrow.string())
        self.input_contract_balance_root = Column(pyarrow.string())
        self.input_contract_state_root = Column(pyarrow.string())
        self.input_contract_tx_pointer = Column(pyarrow.string())
        self.input_contract_contract = Column(pyarrow.string())
        # output contract
        self.output_contract_input_index = Column(pyarrow.int32())
        self.output_contract_balance_root = Column(pyarrow.string())
        self.output_contract_state_root = Column(pyarrow.string())
        # policies
        self.policies_gas_price = Column(pyarrow.uint64())
        self.policies_witness_limit = Column(pyarrow.uint64())
        self.policies_maturity = Column(pyarrow.uint32())
        self.policies_max_fee = Column(pyarrow.uint64())
        # sizes
        self.input_asset_ids_size = Column(pyarrow.int64())
        self.input_contracts_size = Column(pyarrow.int64())
        self.witnesses_size = Column(pyarrow.int64())
        self.storage_slots_size = Column(pyarrow.int64())

    def append(self, block_number: int, tx: Transaction) -> None:
        self.block_number.append(block_number)
        self.transaction_index.append(tx['index'])
        self.hash.append(tx['hash'])

        input_asset_ids = tx.get('inputAssetIds')
        self.input_asset_ids.append(input_asset_ids)
        self.input_asset_ids_size.append(_list_size(input_asset_ids))

        input_contracts = tx.get('inputContracts')
        self.input_contracts.append(input_contracts)
        self.input_contracts_size.append(_list_size(input_contracts))

        witnesses = tx.get('witnesses')
        self.witnesses.append(witnesses)
        self.witnesses_size.append(_list_size(witnesses))

        storage_slots =  tx.get('storageSlots')
        self.storage_slots.append(storage_slots)
        self.storage_slots_size.append(_list_size(storage_slots))

        self.gas_price.append(_to_int(tx.get('gasPrice')))
        self.script_gas_limit.append(_to_int(tx.get('scriptGasLimit')))
        self.maturity.append(tx.get('maturity'))
        self.mint_amount.append(_to_int(tx.get('mintAmount')))
        self.mint_asset_id.append(tx.get('mintAssetId'))
        self.tx_pointer.append(tx.get('txPointer'))
        self.is_script.append(tx['isScript'])
        self.is_create.append(tx['isCreate'])
        self.is_mint.append(tx['isMint'])
        self.type.append(tx['type'])
        self.receipts_root.append(tx.get('receiptsRoot'))
        self.script.append(tx.get('script'))
        self.script_data.append(tx.get('scriptData'))
        self.bytecode_witness_index.append(tx.get('bytecodeWitnessIndex'))
        self.bytecode_length.append(_to_int(tx.get('bytecodeLength')))
        self.salt.append(tx.get('salt'))
        self.raw_payload.append(tx.get('rawPayload'))

        status_type = tx['status']['type']
        assert status_type in ('SuccessStatus', 'FailureStatus', 'SqueezedOutStatus')
        self.status.append(status_type)

        if status_type == 'SuccessStatus':
            self.success_status_transaction_id.append(tx['status'].get('transactionId'))
            self.success_status_time.append(int(tx['status']['time']))
            if program_state := tx['status'].get('programState'):
                self.success_status_program_state_return_type.append(program_state['returnType'])
                self.success_status_program_state_data.append(program_state['data'])
            else:
                self.success_status_program_state_return_type.append(None)
                self.success_status_program_state_data.append(None)
        else:
            self.success_status_transaction_id.append(None)
            self.success_status_time.append(None)
            self.success_status_program_state_return_type.append(None)
            self.success_status_program_state_data.append(None)

        if status_type == 'FailureStatus':
            self.squeezed_out_status_reason.append(tx['status']['reason'])
        else:
            self.squeezed_out_status_reason.append(None)

        if status_type == 'SqueezedOutStatus':
            self.failure_status_transaction_id.append(tx['status']['transactionId'])
            self.failure_status_time.append(int(tx['status']['time']))
            self.failure_status_reason.append(tx['status']['reason'])
            if program_state := tx['status'].get('programState'):
                self.failure_status_program_state_return_type.append(program_state['returnType'])
                self.failure_status_program_state_data.append(program_state['data'])
            else:
                self.failure_status_program_state_return_type.append(None)
                self.failure_status_program_state_data.append(None)
        else:
            self.failure_status_transaction_id.append(None)
            self.failure_status_time.append(None)
            self.failure_status_reason.append(None)
            self.failure_status_program_state_return_type.append(None)
            self.failure_status_program_state_data.append(None)

        if input_contract := tx.get('inputContract'):
            self.input_contract_utxo_id.append(input_contract['utxoId'])
            self.input_contract_balance_root.append(input_contract['balanceRoot'])
            self.input_contract_state_root.append(input_contract['stateRoot'])
            self.input_contract_tx_pointer.append(input_contract['txPointer'])
            self.input_contract_contract.append(input_contract['contract'])
        else:
            self.input_contract_utxo_id.append(None)
            self.input_contract_balance_root.append(None)
            self.input_contract_state_root.append(None)
            self.input_contract_tx_pointer.append(None)
            self.input_contract_contract.append(None)

        if output_contract := tx.get('outputContract'):
            self.output_contract_input_index.append(output_contract['inputIndex'])
            self.output_contract_balance_root.append(output_contract['balanceRoot'])
            self.output_contract_state_root.append(output_contract['stateRoot'])
        else:
            self.output_contract_input_index.append(None)
            self.output_contract_balance_root.append(None)
            self.output_contract_state_root.append(None)

        if policies := tx.get('policies'):
            self.policies_gas_price.append(_to_int(policies.get('gasPrice')))
            self.policies_witness_limit.append(_to_int(policies.get('witnessLimit')))
            self.policies_maturity.append(policies.get('maturity'))
            self.policies_max_fee.append(_to_int(policies.get('maxFee')))
        else:
            self.policies_gas_price.append(None)
            self.policies_witness_limit.append(None)
            self.policies_maturity.append(None)
            self.policies_max_fee.append(None)


class InputTable(TableBuilder):
    def __init__(self):
        self.block_number = Column(pyarrow.int32())
        self.transaction_index = Column(pyarrow.int32())
        self.index = Column(pyarrow.int32())
        self.type = Column(pyarrow.string())
        # coin input
        self.coin_utxo_id = Column(pyarrow.string())
        self.coin_owner = Column(pyarrow.string())
        self.coin_amount = Column(pyarrow.uint64())
        self.coin_asset_id = Column(pyarrow.string())
        self.coin_tx_pointer = Column(pyarrow.string())
        self.coin_witness_index = Column(pyarrow.int32())
        self.coin_maturity = Column(pyarrow.uint32())
        self.coin_predicate_gas_used = Column(pyarrow.uint64())
        self.coin_predicate = Column(pyarrow.string())
        self.coin_predicate_data = Column(pyarrow.string())
        self._coin_predicate_root = Column(pyarrow.string())
        # contract input
        self.contract_utxo_id = Column(pyarrow.string())
        self.contract_balance_root = Column(pyarrow.string())
        self.contract_state_root = Column(pyarrow.string())
        self.contract_tx_pointer = Column(pyarrow.string())
        self.contract_contract = Column(pyarrow.string())
        # message input
        self.message_sender = Column(pyarrow.string())
        self.message_recipient = Column(pyarrow.string())
        self.message_amount = Column(pyarrow.uint64())
        self.message_nonce = Column(pyarrow.string())
        self.message_witness_index = Column(pyarrow.int32())
        self.message_predicate_gas_used = Column(pyarrow.uint64())
        self.message_data = Column(pyarrow.string())
        self.message_predicate = Column(pyarrow.string())
        self.message_predicate_data = Column(pyarrow.string())
        self._message_predicate_root = Column(pyarrow.string())

    def append(self, block_number: int, input: TransactionInput) -> None:
        self.block_number.append(block_number)
        self.transaction_index.append(input['transactionIndex'])
        self.index.append(input['index'])

        assert input['type'] in ('InputCoin', 'InputContract', 'InputMessage')
        self.type.append(input['type'])

        if input['type'] == 'InputCoin':
            self.coin_utxo_id.append(input['utxoId'])
            self.coin_owner.append(input['owner'])
            self.coin_amount.append(int(input['amount']))
            self.coin_asset_id.append(input['assetId'])
            self.coin_tx_pointer.append(input['txPointer'])
            self.coin_witness_index.append(input['witnessIndex'])
            self.coin_maturity.append(input['maturity'])
            self.coin_predicate_gas_used.append(int(input['predicateGasUsed']))
            self.coin_predicate.append(input['predicate'])
            self.coin_predicate_data.append(input['predicateData'])
            self._coin_predicate_root.append(input.get('_predicateRoot'))
        else:
            self.coin_utxo_id.append(None)
            self.coin_owner.append(None)
            self.coin_amount.append(None)
            self.coin_asset_id.append(None)
            self.coin_tx_pointer.append(None)
            self.coin_witness_index.append(None)
            self.coin_maturity.append(None)
            self.coin_predicate_gas_used.append(None)
            self.coin_predicate.append(None)
            self.coin_predicate_data.append(None)
            self._coin_predicate_root.append(None)

        if input['type'] == 'InputContract':
            self.contract_utxo_id.append(input['utxoId'])
            self.contract_balance_root.append(input['balanceRoot'])
            self.contract_state_root.append(input['stateRoot'])
            self.contract_tx_pointer.append(input['txPointer'])
            self.contract_contract.append(input['contract'])
        else:
            self.contract_utxo_id.append(None)
            self.contract_balance_root.append(None)
            self.contract_state_root.append(None)
            self.contract_tx_pointer.append(None)
            self.contract_contract.append(None)

        if input['type'] == 'InputMessage':
            self.message_sender.append(input['sender'])
            self.message_recipient.append(input['recipient'])
            self.message_amount.append(int(input['amount']))
            self.message_nonce.append(input['nonce'])
            self.message_witness_index.append(input['witnessIndex'])
            self.message_predicate_gas_used.append(int(input['predicateGasUsed']))
            self.message_data.append(input['data'])
            self.message_predicate.append(input['predicate'])
            self.message_predicate_data.append(input['predicateData'])
            self._message_predicate_root.append(input.get('_predicateRoot'))
        else:
            self.message_sender.append(None)
            self.message_recipient.append(None)
            self.message_amount.append(None)
            self.message_nonce.append(None)
            self.message_witness_index.append(None)
            self.message_predicate_gas_used.append(None)
            self.message_data.append(None)
            self.message_predicate.append(None)
            self.message_predicate_data.append(None)
            self._message_predicate_root.append(None)


class OutputTable(TableBuilder):
    def __init__(self):
        self.block_number = Column(pyarrow.int32())
        self.transaction_index = Column(pyarrow.int32())
        self.index = Column(pyarrow.int32())
        self.type = Column(pyarrow.string())
        # coin output
        self.coin_to = Column(pyarrow.string())
        self.coin_amount = Column(pyarrow.uint64())
        self.coin_asset_id = Column(pyarrow.string())
        # contract output
        self.contract_input_index = Column(pyarrow.int32())
        self.contract_balance_root = Column(pyarrow.string())
        self.contract_state_root = Column(pyarrow.string())
        # change output
        self.change_to = Column(pyarrow.string())
        self.change_amount = Column(pyarrow.uint64())
        self.change_asset_id = Column(pyarrow.string())
        # variable output
        self.variable_to = Column(pyarrow.string())
        self.variable_amount = Column(pyarrow.uint64())
        self.variable_asset_id = Column(pyarrow.string())
        # contract created
        self.contract_created_contract_id = Column(pyarrow.string())
        self.contract_created_contract_bytecode = Column(pyarrow.string())
        self.contract_created_contract_salt = Column(pyarrow.string())
        self.contract_created_state_root = Column(pyarrow.string())

    def append(self, block_number: int, output: TransactionOutput) -> None:
        self.block_number.append(block_number)
        self.transaction_index.append(output['transactionIndex'])
        self.index.append(output['index'])

        assert output['type'] in ('CoinOutput', 'ContractOutput', 'ChangeOutput', 'VariableOutput', 'ContractCreated')
        self.type.append(output['type'])

        if output['type'] == 'CoinOutput':
            self.coin_to.append(output['to'])
            self.coin_amount.append(int(output['amount']))
            self.coin_asset_id.append(output['assetId'])
        else:
            self.coin_to.append(None)
            self.coin_amount.append(None)
            self.coin_asset_id.append(None)

        if output['type'] == 'ContractOutput':
            self.contract_input_index.append(output['inputIndex'])
            self.contract_balance_root.append(output['balanceRoot'])
            self.contract_state_root.append(output['stateRoot'])
        else:
            self.contract_input_index.append(None)
            self.contract_balance_root.append(None)
            self.contract_state_root.append(None)

        if output['type'] == 'ChangeOutput':
            self.change_to.append(output['to'])
            self.change_amount.append(int(output['amount']))
            self.change_asset_id.append(output['assetId'])
        else:
            self.change_to.append(None)
            self.change_amount.append(None)
            self.change_asset_id.append(None)

        if output['type'] == 'VariableOutput':
            self.variable_to.append(output['to'])
            self.variable_amount.append(int(output['amount']))
            self.variable_asset_id.append(output['assetId'])
        else:
            self.variable_to.append(None)
            self.variable_amount.append(None)
            self.variable_asset_id.append(None)

        if output['type'] == 'ContractCreated':
            self.contract_created_contract_id.append(output['contract']['id'])
            self.contract_created_contract_bytecode.append(output['contract']['bytecode'])
            self.contract_created_contract_salt.append(output['contract']['salt'])
            self.contract_created_state_root.append(output['stateRoot'])
        else:
            self.contract_created_contract_id.append(None)
            self.contract_created_contract_bytecode.append(None)
            self.contract_created_contract_salt.append(None)
            self.contract_created_state_root.append(None)


class ReceiptTable(TableBuilder):
    def __init__(self):
        self.block_number = Column(pyarrow.int32())
        self.transaction_index = Column(pyarrow.int32())
        self.index = Column(pyarrow.int32())
        self.contract = Column(pyarrow.string())
        self.pc = Column(pyarrow.uint64())
        self.__dict__['is'] = Column(pyarrow.uint64())
        self.to = Column(pyarrow.string())
        self.to_address = Column(pyarrow.string())
        self.amount = Column(pyarrow.uint64())
        self.asset_id = Column(pyarrow.string())
        self.gas = Column(pyarrow.uint64())
        self.param1 = Column(pyarrow.uint64())
        self.param2 = Column(pyarrow.uint64())
        self.val = Column(pyarrow.uint64())
        self.ptr = Column(pyarrow.uint64())
        self.digest = Column(pyarrow.string())
        self.reason = Column(pyarrow.uint64())
        self.ra = Column(pyarrow.uint64())
        self.rb = Column(pyarrow.uint64())
        self.rc = Column(pyarrow.uint64())
        self.rd = Column(pyarrow.uint64())
        self.len = Column(pyarrow.uint64())
        self.receipt_type = Column(pyarrow.string())
        self.result = Column(pyarrow.uint64())
        self.gas_used = Column(pyarrow.uint64())
        self.data = Column(pyarrow.string())
        self.sender = Column(pyarrow.string())
        self.recipient = Column(pyarrow.string())
        self.nonce = Column(pyarrow.string())
        self.contract_id = Column(pyarrow.string())
        self.sub_id = Column(pyarrow.string())

    def append(self, block_number: int, receipt: Receipt) -> None:
        self.block_number.append(block_number)
        self.transaction_index.append(receipt['transactionIndex'])
        self.index.append(receipt['index'])
        self.contract.append(receipt.get('contract'))
        self.pc.append(_to_int(receipt.get('pc')))
        self.__dict__['is'].append(_to_int(receipt.get('is')))
        self.to.append(receipt.get('to'))
        self.to_address.append(receipt.get('toAddress'))
        self.amount.append(_to_int(receipt.get('amount')))
        self.asset_id.append(receipt.get('assetId'))
        self.gas.append(_to_int(receipt.get('gas')))
        self.param1.append(_to_int(receipt.get('param1')))
        self.param2.append(_to_int(receipt.get('param2')))
        self.val.append(_to_int(receipt.get('val')))
        self.ptr.append(_to_int(receipt.get('ptr')))
        self.digest.append(receipt.get('digest'))
        self.reason.append(_to_int(receipt.get('reason')))
        self.ra.append(_to_int(receipt.get('ra')))
        self.rb.append(_to_int(receipt.get('rb')))
        self.rc.append(_to_int(receipt.get('rc')))
        self.rd.append(_to_int(receipt.get('rd')))
        self.len.append(_to_int(receipt.get('len')))
        self.receipt_type.append(receipt.get('receiptType'))
        self.result.append(_to_int(receipt.get('result')))
        self.gas_used.append(_to_int(receipt.get('gas_used')))
        self.data.append(receipt.get('data'))
        self.sender.append(receipt.get('sender'))
        self.recipient.append(receipt.get('recipient'))
        self.nonce.append(receipt.get('nonce'))
        self.contract_id.append(receipt.get('contractId'))
        self.sub_id.append(receipt.get('subId'))


class ParquetWriter(BaseParquetWriter):
    def __init__(self):
        self.blocks = BlockTable()
        self.transactions = TransactionTable()
        self.inputs = InputTable()
        self.outputs = OutputTable()
        self.receipts = ReceiptTable()

    def push(self, block: Block) -> None:
        block_number = block['header']['height']

        self.blocks.append(block['header'])

        for tx in block['transactions']:
            self.transactions.append(block_number, tx)

        for input in block['inputs']:
            self.inputs.append(block_number, input)

        for output in block['outputs']:
            self.outputs.append(block_number, output)

        for receipt in block['receipts']:
            self.receipts.append(block_number, receipt)

    def _write(self, fs: Fs, tables: dict[str, pyarrow.Table]) -> None:
        write_parquet(fs, tables)

    def get_block_height(self, block: Block) -> int:
        return block['header']['height']

    def get_block_hash(self, block: Block) -> str:
        return block['header']['hash']


def write_parquet(fs: Fs, tables: dict[str, pyarrow.Table]) -> None:
    kwargs = {
        'data_page_size': 32 * 1024,
        'dictionary_pagesize_limit': 192 * 1024,
        'compression': 'zstd',
        'write_page_index': True,
        'write_batch_size': 50
    }

    transactions = tables['transactions']
    transactions = transactions.sort_by([
        ('type', 'ascending'),
        ('block_number', 'ascending'),
        ('transaction_index', 'ascending'),
    ])
    transactions = add_size_column(transactions, 'script_data')
    transactions = add_size_column(transactions, 'raw_payload')
    transactions = add_index_column(transactions)

    fs.write_parquet(
        'transactions.parquet',
        transactions,
        use_dictionary=[
            'type',
            'status',
            'success_status_program_state_return_type',
            'failure_status_program_state_return_type'
        ],
        write_statistics=[
            '_idx',
            'type',
            'block_number',
            'transaction_index',
        ],
        row_group_size=5_000,
        **kwargs
    )

    inputs = tables['inputs']
    inputs = inputs.sort_by([
        ('type', 'ascending'),
        ('block_number', 'ascending'),
        ('transaction_index', 'ascending'),
        ('index', 'ascending')
    ])
    inputs = add_size_column(inputs, 'coin_predicate')
    inputs = add_size_column(inputs, 'message_predicate')
    inputs = add_index_column(inputs)

    fs.write_parquet(
        'inputs.parquet',
        inputs,
        use_dictionary=['type'],
        write_statistics=[
            '_idx',
            'type',
            'block_number',
            'transaction_index',
            'index'
        ],
        row_group_size=5_000,
        **kwargs
    )

    outputs = tables['outputs']
    outputs = outputs.sort_by([
        ('type', 'ascending'),
        ('block_number', 'ascending'),
        ('transaction_index', 'ascending'),
        ('index', 'ascending')
    ])
    outputs = add_size_column(outputs, 'contract_created_contract_bytecode')
    outputs = add_index_column(outputs)

    fs.write_parquet(
        'outputs.parquet',
        outputs,
        use_dictionary=['type'],
        write_statistics=[
            '_idx',
            'type',
            'block_number',
            'transaction_index',
            'index'
        ],
        row_group_size=5_000,
        **kwargs
    )

    receipts = tables['receipts']
    receipts = receipts.sort_by([
        ('receipt_type', 'ascending'),
        ('contract', 'ascending'),
        ('block_number', 'ascending'),
        ('transaction_index', 'ascending'),
        ('index', 'ascending')
    ])
    receipts = add_size_column(receipts, 'data')
    receipts = add_index_column(receipts)

    fs.write_parquet(
        'receipts.parquet',
        receipts,
        use_dictionary=['receipt_type'],
        write_statistics=[
            '_idx',
            'receipt_type',
            'block_number',
            'transaction_index',
            'index'
        ],
        row_group_size=5_000,
        **kwargs
    )

    blocks = tables['blocks']

    fs.write_parquet(
        'blocks.parquet',
        blocks,
        **kwargs
    )


def _list_size(ls: list[str] | None) -> int:
    return 0 if ls is None else sum(len(i) for i in ls)


def _to_int(val: str | None) -> int | None:
    return None if val is None else int(val)
