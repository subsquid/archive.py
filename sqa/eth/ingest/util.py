import binascii
from typing import TypedDict, NotRequired
import math

from trie import HexaryTrie
from Crypto.Hash import keccak
import rlp
from eth_utils.encoding import int_to_big_endian
from eth_keys.datatypes import Signature

from sqa.eth.ingest.model import Qty, Hash32, Transaction, Address20, Block, Log, Receipt


def qty2int(v: Qty) -> int:
    return int(v, 16)


def short_hash(value: Hash32) -> str:
    """Takes first 4 bytes from the hash"""
    assert value.startswith('0x')
    return value[2:10]


def decode_hex(value: str) -> bytes:
    assert value.startswith('0x')
    ascii_hex = value[2:].encode('ascii')
    return binascii.unhexlify(ascii_hex)


def encode_hex(value: bytes | bytearray) -> str:
    binary_hex = binascii.hexlify(value)
    return '0x' + binary_hex.decode('ascii')


def keccak256(buffer) -> bytes:
    k = keccak.new(digest_bits=256)
    return bytes(k.update(buffer).digest())


class TxStatus(TypedDict):
    status: Qty
    contractAddress: NotRequired[Address20]


def get_tx_status_from_traces(tx: Transaction) -> TxStatus | None:
    if frame_result := tx.get('debugFrame_'):
        frame = frame_result['result']
        ok = frame.get('error') is None and frame['type'] != 'INVALID'
        s: TxStatus = {
            'status': '0x1' if ok else '0x0'
        }
        if ok and frame['type'] in ('CREATE', 'CREATE2'):
            s['contractAddress'] = frame['to']
        return s

    replay = tx.get('traceReplay_')
    if replay is not None and (trace := replay.get('trace')):
        top = trace[0]
        assert top['traceAddress'] == []
        ok = top.get('error') is None
        s: TxStatus = {
            'status': '0x1' if ok else '0x0'
        }
        if ok and top['type'] == 'create':
            s['contractAddress'] = top['result']['address']
        return s


def _add_to_bloom(bloom: bytearray, bloom_entry: bytes):
    hash = keccak256(bloom_entry)
    for idx in (0, 2, 4):
        bit_to_set = int.from_bytes(hash[idx:idx+2], "big") & 0x07FF
        bit_index = 0x07FF - bit_to_set
        byte_index = bit_index // 8
        bit_value = 1 << (7 - (bit_index % 8))
        bloom[byte_index] = bloom[byte_index] | bit_value


def logs_bloom(logs: list[Log]) -> str:
    bloom = bytearray(b"\x00" * 256)
    for log in logs:
        _add_to_bloom(bloom, decode_hex(log['address']))
        for topic in log['topics']:
            _add_to_bloom(bloom, decode_hex(topic))
    return encode_hex(bloom)


def _encode_access_list(access_list: list) -> list[list[bytes | list[bytes]]]:
    encoded = []
    for item in access_list:
        address = decode_hex(item['address'])
        keys = []
        for key in item['storageKeys']:
            val = int_to_big_endian(qty2int(key))
            keys.append(b'\x00' * max(0, 32 - len(val)) + val)
        encoded.append([address, keys])
    return encoded


def transactions_root(transactions: list[Transaction]) -> str:
    trie = HexaryTrie({})
    for tx in transactions:
        path = rlp.encode(qty2int(tx['transactionIndex']))
        if tx['type'] == '0x0':
            trie[path] = rlp.encode([
                qty2int(tx['nonce']),
                qty2int(tx['gasPrice']),
                qty2int(tx['gas']),
                decode_hex(tx['to']) if tx['to'] else b'',
                qty2int(tx['value']),
                decode_hex(tx['input']),
                qty2int(tx['v']),
                qty2int(tx['r']),
                qty2int(tx['s'])
            ])
        elif tx['type'] == '0x1':
            trie[path] = b'\x01' + rlp.encode([
                qty2int(tx['chainId']),
                qty2int(tx['nonce']),
                qty2int(tx['gasPrice']),
                qty2int(tx['gas']),
                decode_hex(tx['to']) if tx['to'] else b'',
                qty2int(tx['value']),
                decode_hex(tx['input']),
                _encode_access_list(tx.get('accessList', [])),
                qty2int(tx['v']),
                qty2int(tx['r']),
                qty2int(tx['s'])
            ])
        elif tx['type'] == '0x2':
            trie[path] = b'\x02' + rlp.encode([
                qty2int(tx['chainId']),
                qty2int(tx['nonce']),
                qty2int(tx['maxPriorityFeePerGas']),
                qty2int(tx['maxFeePerGas']),
                qty2int(tx['gas']),
                decode_hex(tx['to']) if tx['to'] else b'',
                qty2int(tx['value']),
                decode_hex(tx['input']),
                _encode_access_list(tx.get('accessList', [])),
                qty2int(tx['v']),
                qty2int(tx['r']),
                qty2int(tx['s'])
            ])
        elif tx['type'] == '0x3':
            # https://eips.ethereum.org/EIPS/eip-4844
            trie[path] = b'\x03' + rlp.encode([
                qty2int(tx['chainId']),
                qty2int(tx['nonce']),
                qty2int(tx['maxPriorityFeePerGas']),
                qty2int(tx['maxFeePerGas']),
                qty2int(tx['gas']),
                decode_hex(tx['to']) if tx['to'] else b'',
                qty2int(tx['value']),
                decode_hex(tx['input']),
                _encode_access_list(tx.get('accessList', [])),
                qty2int(tx['maxFeePerBlobGas']),
                [decode_hex(h) for h in tx['blobVersionedHashes']],
                qty2int(tx['yParity']) if 'yParity' in tx else qty2int(tx['v']),
                qty2int(tx['r']),
                qty2int(tx['s']),
            ])
        elif tx['type'] == '0x64':
            # https://github.com/OffchainLabs/go-ethereum/blob/7503143fd13f73e46a966ea2c42a058af96f7fcf/core/types/arb_types.go#L338
            trie[path] = b'\x64' + rlp.encode([
                qty2int(tx['chainId']),
                decode_hex(tx['requestId']),
                decode_hex(tx['from']),
                decode_hex(tx['to']),
                qty2int(tx['value'])
            ])
        elif tx['type'] == '0x65':
            # https://github.com/OffchainLabs/go-ethereum/blob/7503143fd13f73e46a966ea2c42a058af96f7fcf/core/types/arb_types.go#L43
            raise NotImplementedError('cannot encode tx with type 0x65')
            # trie[path] = b'\x65' + rlp.encode([
            #     qty2int(tx['chainId']),
            #     decode_hex(tx['from']),
            #     qty2int(tx['nonce']),
            #     qty2int(tx['gasPrice']),
            #     qty2int(tx['gas']),
            #     decode_hex(tx['to']) if tx['to'] else b'',
            #     qty2int(tx['value']),
            #     decode_hex(tx['input'])
            # ])
        elif tx['type'] == '0x66':
            # https://github.com/OffchainLabs/go-ethereum/blob/7503143fd13f73e46a966ea2c42a058af96f7fcf/core/types/arb_types.go#L104
            trie[path] = b'\x66' + rlp.encode([
                qty2int(tx['chainId']),
                decode_hex(tx['requestId']),
                decode_hex(tx['from']),
                qty2int(tx['gasPrice']),
                qty2int(tx['gas']),
                decode_hex(tx['to']) if tx['to'] else b'',
                qty2int(tx['value']),
                decode_hex(tx['input'])
            ])
        elif tx['type'] == '0x68':
            # https://github.com/OffchainLabs/go-ethereum/blob/7503143fd13f73e46a966ea2c42a058af96f7fcf/core/types/arb_types.go#L161
            trie[path] = b'\x68' + rlp.encode([
                qty2int(tx['chainId']),
                qty2int(tx['nonce']),
                decode_hex(tx['from']),
                qty2int(tx['gasPrice']),
                qty2int(tx['gas']),
                decode_hex(tx['to']) if tx['to'] else b'',
                qty2int(tx['value']),
                decode_hex(tx['input']),
                decode_hex(tx['ticketId']),
                decode_hex(tx['refundTo']),
                qty2int(tx['maxRefund']),
                qty2int(tx['submissionFeeRefund']),
            ])
        elif tx['type'] == '0x69':
            # https://github.com/OffchainLabs/go-ethereum/blob/7503143fd13f73e46a966ea2c42a058af96f7fcf/core/types/arb_types.go#L232
            trie[path] = b'\x69' + rlp.encode([
                qty2int(tx['chainId']),
                decode_hex(tx['requestId']),
                decode_hex(tx['from']),
                qty2int(tx['l1BaseFee']),
                qty2int(tx['depositValue']),
                qty2int(tx['gasPrice']),
                qty2int(tx['gas']),
                decode_hex(tx['retryTo']) if 'retryTo' in tx else b'',
                qty2int(tx['retryValue']),
                decode_hex(tx['beneficiary']),
                qty2int(tx['maxSubmissionFee']),
                decode_hex(tx['refundTo']),
                decode_hex(tx['retryData']),
            ])
        elif tx['type'] == '0x6a':
            # https://github.com/OffchainLabs/go-ethereum/blob/7503143fd13f73e46a966ea2c42a058af96f7fcf/core/types/arb_types.go#L387
            trie[path] = b'\x6a' + rlp.encode([
                qty2int(tx['chainId']),
                decode_hex(tx['input']),
            ])
        elif tx['type'] == '0x7e':
            # https://github.com/ethereum-optimism/optimism/blob/9ff3ebb3983be52c3ca189423ae7b4aec94e0fde/specs/deposits.md#the-deposited-transaction-type
            trie[path] = b'\x7e' + rlp.encode([
                decode_hex(tx['sourceHash']),
                decode_hex(tx['from']),
                decode_hex(tx['to']) if tx['to'] else b'',
                qty2int(tx['mint']),
                qty2int(tx['value']),
                qty2int(tx['gas']),
                False,
                decode_hex(tx['input']),
            ])
        else:
            raise Exception(f'Unknown tx type {tx["type"]}')
    return encode_hex(trie.root_hash)


def _encode_logs(logs: list[Log]):
    encoded = []
    for log in logs:
        address = decode_hex(log['address'])
        topics = []
        for topic in log['topics']:
            topics.append(decode_hex(topic))
        data = decode_hex(log['data'])
        encoded.append([address, topics, data])
    return encoded


def receipts_root(receipts: list[Receipt]) -> str:
    trie = HexaryTrie({})
    for receipt in receipts:
        path = rlp.encode(qty2int(receipt['transactionIndex']))
        type_ = b'' if receipt['type'] == '0x0' else rlp.encode(qty2int(receipt['type']))
        if receipt['type'] == '0x7e':
            # https://github.com/ethereum-optimism/specs/blob/main/specs/protocol/deposits.md#deposit-receipt
            receipt_trie = rlp.encode([
                qty2int(receipt['status']),
                qty2int(receipt['cumulativeGasUsed']),
                decode_hex(logs_bloom(receipt['logs'])),
                _encode_logs(receipt['logs']),
                qty2int(receipt['depositNonce']),
                int('depositReceiptVersion' in receipt),
            ])
        else:
            receipt_trie = rlp.encode([
                qty2int(receipt['status']),
                qty2int(receipt['cumulativeGasUsed']),
                decode_hex(logs_bloom(receipt['logs'])),
                _encode_logs(receipt['logs']),
            ])
        trie[path] = type_ + receipt_trie
    return encode_hex(trie.root_hash)


def get_polygon_bor_tx_hash(block_num: int, block_hash: str):
    tx_receipt_key = b'matic-bor-receipt-' + block_num.to_bytes(8) + decode_hex(block_hash)
    return encode_hex(keccak256(tx_receipt_key))


def _serialize_transaction(tx: Transaction):
    if tx['type'] == '0x0':
        fields = [
            qty2int(tx['nonce']),
            qty2int(tx['gasPrice']),
            qty2int(tx['gas']),
            decode_hex(tx['to']) if tx['to'] else b'',
            qty2int(tx['value']),
            decode_hex(tx['input']),
        ]

        chain_id = qty2int(tx['chainId']) if 'chainId' in tx else None
        if chain_id:
            fields.extend([chain_id, 0, 0])

        return rlp.encode(fields)
    elif tx['type'] == '0x1':
        return b'\x01' + rlp.encode([
            qty2int(tx['chainId']),
            qty2int(tx['nonce']),
            qty2int(tx['gasPrice']),
            qty2int(tx['gas']),
            decode_hex(tx['to']) if tx['to'] else b'',
            qty2int(tx['value']),
            decode_hex(tx['input']),
            _encode_access_list(tx.get('accessList', [])),
        ])
    elif tx['type'] == '0x2':
        return b'\x02' + rlp.encode([
            qty2int(tx['chainId']),
            qty2int(tx['nonce']),
            qty2int(tx['maxPriorityFeePerGas']),
            qty2int(tx['maxFeePerGas']),
            qty2int(tx['gas']),
            decode_hex(tx['to']) if tx['to'] else b'',
            qty2int(tx['value']),
            decode_hex(tx['input']),
            _encode_access_list(tx.get('accessList', [])),
        ])
    elif tx['type'] == '0x3':
        return b'\x03' + rlp.encode([
            qty2int(tx['chainId']),
            qty2int(tx['nonce']),
            qty2int(tx['maxPriorityFeePerGas']),
            qty2int(tx['maxFeePerGas']),
            qty2int(tx['gas']),
            decode_hex(tx['to']) if tx['to'] else b'',
            qty2int(tx['value']),
            decode_hex(tx['input']),
            _encode_access_list(tx.get('accessList', [])),
            qty2int(tx['maxFeePerBlobGas']),
            [decode_hex(h) for h in tx['blobVersionedHashes']],
        ])
    elif tx['type'] == '0x64':
        # https://github.com/OffchainLabs/go-ethereum/blob/7503143fd13f73e46a966ea2c42a058af96f7fcf/core/types/arb_types.go#L338
        raise NotImplementedError('cannot encode tx with type 0x64')
    elif tx['type'] == '0x65':
        # https://github.com/OffchainLabs/go-ethereum/blob/7503143fd13f73e46a966ea2c42a058af96f7fcf/core/types/arb_types.go#L43
        raise NotImplementedError('cannot encode tx with type 0x65')
    elif tx['type'] == '0x66':
        # https://github.com/OffchainLabs/go-ethereum/blob/7503143fd13f73e46a966ea2c42a058af96f7fcf/core/types/arb_types.go#L104
        raise NotImplementedError('cannot encode tx with type 0x66')
    elif tx['type'] == '0x68':
        # https://github.com/OffchainLabs/go-ethereum/blob/7503143fd13f73e46a966ea2c42a058af96f7fcf/core/types/arb_types.go#L161
        raise NotImplementedError('cannot encode tx with type 0x68')
    elif tx['type'] == '0x69':
        # https://github.com/OffchainLabs/go-ethereum/blob/7503143fd13f73e46a966ea2c42a058af96f7fcf/core/types/arb_types.go#L232
        raise NotImplementedError('cannot encode tx with type 0x69')
    elif tx['type'] == '0x6a':
        # https://github.com/OffchainLabs/go-ethereum/blob/7503143fd13f73e46a966ea2c42a058af96f7fcf/core/types/arb_types.go#L387
        raise NotImplementedError('cannot encode tx with type 0x6a')
    elif tx['type'] == '0x7e':
        # https://github.com/ethereum-optimism/optimism/blob/9ff3ebb3983be52c3ca189423ae7b4aec94e0fde/specs/deposits.md#the-deposited-transaction-type
        raise NotImplementedError('cannot encode tx with type 0x7e')
    else:
        raise Exception(f'Unknown tx type {tx["type"]}')


def _create_signature(tx: Transaction):
    v = qty2int(tx['v'])
    if v not in (0, 1):
        if 'chainId' in tx:
            chain_id = qty2int(tx['chainId'])
        else:
            chain_id = math.floor((v - 35) / 2)
        v -= chain_id * 2 + 35

    vrs = (v, qty2int(tx['r']), qty2int(tx['s']))
    return Signature(vrs=vrs)


def recover_tx_sender(tx: Transaction):
    message = _serialize_transaction(tx)
    signature = _create_signature(tx)
    public_key = signature.recover_public_key_from_msg(message)
    address = public_key.to_canonical_address()
    return encode_hex(address)


def block_hash(block: Block) -> str:
    fields = [
        decode_hex(block['parentHash']),
        decode_hex(block['sha3Uncles']),
        decode_hex(block['miner']),
        decode_hex(block['stateRoot']),
        decode_hex(block['transactionsRoot']),
        decode_hex(block['receiptsRoot']),
        decode_hex(block['logsBloom']),
        qty2int(block['difficulty']),
        qty2int(block['number']),
        qty2int(block['gasLimit']),
        qty2int(block['gasUsed']),
        qty2int(block['timestamp']),
        decode_hex(block['extraData']),
        decode_hex(block['mixHash']),
        decode_hex(block['nonce'])
    ]

    # https://eips.ethereum.org/EIPS/eip-1559#block-hash-changing
    if 'baseFeePerGas' in block:
        fields.append(qty2int(block['baseFeePerGas']))

    # https://eips.ethereum.org/EIPS/eip-4895#new-field-in-the-execution-payload-header-withdrawals-root
    if 'withdrawalsRoot' in block:
        fields.append(decode_hex(block['withdrawalsRoot']))

    # https://eips.ethereum.org/EIPS/eip-4844#header-extension
    if 'blobGasUsed' in block:
        fields.append(qty2int(block['blobGasUsed']))
        fields.append(qty2int(block['excessBlobGas']))

    # https://eips.ethereum.org/EIPS/eip-4788#block-structure-and-validity
    if 'parentBeaconBlockRoot' in block:
        fields.append(decode_hex(block['parentBeaconBlockRoot']))

    encoded = rlp.encode(fields)
    return encode_hex(keccak256(encoded))
