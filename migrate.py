from etha.log import init_logging
if __name__ == '__main__':
    init_logging()

import logging
import argparse
import collections

import pyarrow
import pyarrow.parquet

from etha.fs import S3Fs, s3fs
from etha.ingest.writer import ParquetWriter, ArrowDataBatch
from etha.layout import get_chunks, ChunkWriter
from etha.ingest.column import Column
from etha.ingest.tables import _to_sighash

LOG = logging.getLogger(__name__)


def chunk_check(filelist: list[str]) -> bool:
    return 'blocks.parquet' in filelist


def extend_logs_table(batch: ArrowDataBatch) -> pyarrow.Table:
    logs = batch.logs
    tx = batch.transactions

    if 'transaction_hash' not in logs.column_names:
        tx_hashes = collections.defaultdict(dict)
        for (block_number, transaction_index, hash) in zip(tx['block_number'], tx['transaction_index'], tx['hash']):
            block_number = block_number.as_py()
            transaction_index = transaction_index.as_py()
            hash = hash.as_py()
            tx_hashes[block_number][transaction_index] = hash

        transaction_hash = Column(pyarrow.string())
        for (block_number, transaction_index) in zip(logs['block_number'], logs['transaction_index']):
            block_number = block_number.as_py()
            transaction_index = transaction_index.as_py()
            transaction_hash.append(tx_hashes[block_number][transaction_index])
        logs = logs.add_column(3, 'transaction_hash', transaction_hash.build())

    return logs


def extend_transactions_table(batch: ArrowDataBatch) -> pyarrow.Table:
    traces = batch.traces
    tx = batch.transactions

    if 'contract_address' not in tx.column_names:
        tx_addresses = collections.defaultdict(dict)
        for (type, address, transaction_index, block_number, trace_address) in zip(
            traces['type'],
            traces['create_result_address'],
            traces['transaction_index'],
            traces['block_number'],
            traces['trace_address'],
        ):
            type = type.as_py()
            address = address.as_py()
            trace_address = trace_address.as_py()
            block_number = block_number.as_py()
            transaction_index = transaction_index.as_py()
            if type == 'create' and trace_address == []:
                tx_addresses[block_number][transaction_index] = address

        contract_address = Column(pyarrow.string())
        for (block_number, transaction_index) in zip(tx['block_number'], tx['transaction_index']):
            block_number = block_number.as_py()
            transaction_index = transaction_index.as_py()
            if block_addresses := tx_addresses.get(block_number):
                contract_address.append(block_addresses.get(transaction_index))
            else:
                contract_address.append(None)
        tx = tx.add_column(21, 'contract_address', contract_address.build())

    return tx


def extend_traces_table(batch: ArrowDataBatch) -> pyarrow.Table:
    traces = batch.traces

    if 'revert_reason' not in traces.column_names:
        revert_reason = pyarrow.array(
            (None for _ in range(0, traces.shape[0])),
            type=pyarrow.string()
        )
        traces = traces.add_column(6, 'revert_reason', revert_reason)

    if 'call_sighash' not in traces.column_names:
        call_sighash = Column(pyarrow.string())
        for ty, inp in zip(traces['type'], traces['call_input']):
            ty = ty.as_py()
            inp = inp.as_py()
            if ty == 'call':
                call_sighash.append(_to_sighash(inp))
            else:
                call_sighash.append(None)
        idx = traces.column_names.index('call_input') + 1
        traces = traces.add_column(idx, 'call_sighash', call_sighash.build())

    return traces


class ParquetReader:
    def __init__(self, bucket: str, fs: s3fs.S3FileSystem):
        self._bucket = bucket
        self._fs = fs

    def read_table(self, *segments):
        source = '/'.join([self._bucket, *segments])
        return pyarrow.parquet.read_table(source, filesystem=self._fs)


def main():
    program = argparse.ArgumentParser(description='Migrate data from one bucket to another')

    program.add_argument('--source-bucket', required=True)
    program.add_argument('--source-access-key', required=True)
    program.add_argument('--source-secret-key', required=True)
    program.add_argument('--source-endpoint', required=True)

    program.add_argument('--destination-bucket', required=True)
    program.add_argument('--destination-access-key', required=True)
    program.add_argument('--destination-secret-key', required=True)
    program.add_argument('--destination-endpoint', required=True)

    args = program.parse_args()

    client_kwargs = {'endpoint_url': args.source_endpoint}
    config_kwargs = {'read_timeout': 120}
    ss3 = s3fs.S3FileSystem(key=args.source_access_key, secret=args.source_secret_key,
                            client_kwargs=client_kwargs, config_kwargs=config_kwargs)

    client_kwargs = {'endpoint_url': args.destination_endpoint}
    config_kwargs = {'read_timeout': 120}
    ds3 = s3fs.S3FileSystem(key=args.destination_access_key, secret=args.destination_secret_key,
                            client_kwargs=client_kwargs, config_kwargs=config_kwargs)

    sfs = S3Fs(ss3, args.source_bucket)
    dfs = S3Fs(ds3, args.destination_bucket)

    chunk_writer = ChunkWriter(dfs, chunk_check)
    block_writer = ParquetWriter(dfs, chunk_writer, with_traces=True, with_statediffs=True)
    next_block = chunk_writer.next_block
    LOG.info(f'next block is {next_block}')

    reader = ParquetReader(args.source_bucket, ss3)
    for chunk in get_chunks(sfs, next_block):
        LOG.info(f'processing chunk {chunk.first_block}-{chunk.last_block}')

        path = chunk.path()
        batch = ArrowDataBatch(
            blocks=reader.read_table(path, 'blocks.parquet'),
            transactions=reader.read_table(path, 'transactions.parquet'),
            logs=reader.read_table(path, 'logs.parquet'),
            traces=reader.read_table(path, 'traces.parquet'),
            statediffs=reader.read_table(path, 'statediffs.parquet'),
            bytesize=0
        )

        logs = extend_logs_table(batch)
        transactions = extend_transactions_table(batch)
        traces = extend_traces_table(batch)

        batch = ArrowDataBatch(
            blocks=batch.blocks,
            transactions=transactions,
            logs=logs,
            traces=traces,
            statediffs=batch.statediffs,
            bytesize=0
        )
        block_writer.write(batch)


if __name__ == '__main__':
    main()
