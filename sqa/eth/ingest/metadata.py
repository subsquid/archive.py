def generate_metadata(with_traces: bool, with_statediffs: bool):
    tables = [
        {
            'name': 'blocks',
            'columns': [
                {'name': 'number'},
                {'name': 'hash'},
                {'name': 'parent_hash'},
                {'name': 'nonce', 'constraints': {'nullable': True}},
                {'name': 'sha3_uncles', 'constraints': {'nullable': True}},
                {'name': 'logs_bloom'},
                {'name': 'transactions_root'},
                {'name': 'state_root'},
                {'name': 'receipts_root'},
                {'name': 'mix_hash', 'constraints': {'nullable': True}},
                {'name': 'miner'},
                {'name': 'difficulty', 'constraints': {'nullable': True}},
                {'name': 'total_difficulty', 'constraints': {'nullable': True}},
                {'name': 'extra_data'},
                {'name': 'size'},
                {'name': 'gas_used'},
                {'name': 'gas_limit'},
                {'name': 'timestamp'},
                {'name': 'base_fee_per_gas', 'constraints': {'nullable': True}},
                {'name': 'l1_block_number', 'constraints': {'nullable': True}},
                {'name': 'blob_gas_used', 'constraints': {'nullable': True}},
                {'name': 'excess_blob_gas', 'constraints': {'nullable': True}},
            ]
        },
        {
            'name': 'transactions',
            'columns': [
                {'name': 'block_number'},
                {'name': 'from'},
                {'name': 'gas'},
                {'name': 'gas_price', 'constraints': {'nullable': True}},
                {'name': 'max_fee_per_gas', 'constraints': {'nullable': True}},
                {'name': 'max_priority_fee_per_gas', 'constraints': {'nullable': True}},
                {'name': 'hash'},
                {'name': 'input'},
                {'name': 'nonce'},
                {'name': 'to', 'constraints': {'nullable': True}},
                {'name': 'transaction_index'},
                {'name': 'value'},
                {'name': 'v', 'constraints': {'nullable': True}},
                {'name': 'r', 'constraints': {'nullable': True}},
                {'name': 's', 'constraints': {'nullable': True}},
                {'name': 'y_parity', 'constraints': {'nullable': True}},
                {'name': 'chain_id', 'constraints': {'nullable': True}},
                {'name': 'max_fee_per_blob_gas', 'constraints': {'nullable': True}},
                {'name': 'blob_versioned_hashes', 'constraints': {'nullable': True}},
                {'name': 'sighash', 'constraints': {'nullable': True}},
                {'name': 'gas_used'},
                {'name': 'cumulative_gas_used'},
                {'name': 'effective_gas_price', 'constraints': {'nullable': True}},
                {'name': 'type', 'constraints': {'nullable': True}},
                {'name': 'status'},
                {'name': 'contract_address', 'constraints': {'nullable': True}},
                {'name': 'l1_fee', 'constraints': {'nullable': True}},
                {'name': 'l1_fee_scalar', 'constraints': {'nullable': True}},
                {'name': 'l1_gas_price', 'constraints': {'nullable': True}},
                {'name': 'l1_gas_used', 'constraints': {'nullable': True}},
                {'name': 'l1_blob_base_fee', 'constraints': {'nullable': True}},
                {'name': 'l1_blob_base_fee_scalar', 'constraints': {'nullable': True}},
                {'name': 'l1_base_fee_scalar', 'constraints': {'nullable': True}},
            ]
        },
        {
            'name': 'logs',
            'columns': [
                {'name': 'block_number'},
                {'name': 'log_index'},
                {'name': 'transaction_index'},
                {'name': 'transaction_hash'},
                {'name': 'address'},
                {'name': 'data'},
                {'name': 'topic0', 'constraints': {'nullable': True}},
                {'name': 'topic1', 'constraints': {'nullable': True}},
                {'name': 'topic2', 'constraints': {'nullable': True}},
                {'name': 'topic3', 'constraints': {'nullable': True}},
            ]
        },
    ]

    if with_traces:
        tables.append({
            'name': 'traces',
            'columns': [
                {'name': 'block_number'},
                {'name': 'transaction_index'},
                {'name': 'trace_address'},
                {'name': 'subtraces'},
                {'name': 'type'},
                {'name': 'error', 'constraints': {'nullable': True}},
                {'name': 'revert_reason', 'constraints': {'nullable': True}},
                {'name': 'create_from', 'constraints': {'nullable': True}},
                {'name': 'create_value', 'constraints': {'nullable': True}},
                {'name': 'create_gas', 'constraints': {'nullable': True}},
                {'name': 'create_init', 'constraints': {'nullable': True}},
                {'name': 'create_result_gas_used', 'constraints': {'nullable': True}},
                {'name': 'create_result_code', 'constraints': {'nullable': True}},
                {'name': 'create_result_address', 'constraints': {'nullable': True}},
                {'name': 'call_from', 'constraints': {'nullable': True}},
                {'name': 'call_to', 'constraints': {'nullable': True}},
                {'name': 'call_value', 'constraints': {'nullable': True}},
                {'name': 'call_gas', 'constraints': {'nullable': True}},
                {'name': 'call_sighash', 'constraints': {'nullable': True}},
                {'name': 'call_input', 'constraints': {'nullable': True}},
                {'name': 'call_type', 'constraints': {'nullable': True}},
                {'name': 'call_result_gas_used', 'constraints': {'nullable': True}},
                {'name': 'call_result_output', 'constraints': {'nullable': True}},
                {'name': 'suicide_address', 'constraints': {'nullable': True}},
                {'name': 'suicide_refund_address', 'constraints': {'nullable': True}},
                {'name': 'suicide_balance', 'constraints': {'nullable': True}},
                {'name': 'reward_author', 'constraints': {'nullable': True}},
                {'name': 'reward_value', 'constraints': {'nullable': True}},
                {'name': 'reward_type', 'constraints': {'nullable': True}},
            ]
        })

    if with_statediffs:
        tables.append({
            'name': 'statediffs',
            'columns': [
                {'name': 'block_number'},
                {'name': 'transaction_index'},
                {'name': 'address'},
                {'name': 'key'},
                {'name': 'kind'},
                {'name': 'prev', 'constraints': {'nullable': True}},
                {'name': 'next', 'constraints': {'nullable': True}},
            ]
        })

    metadata = {
        'schema': {
            'tables': tables
        }
    }

    return metadata
