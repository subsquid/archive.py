import logging
from functools import cache
from typing import NamedTuple, Optional

import pyarrow

from etha.ingest.model import Block
from etha.ingest.tables import BlockTableBuilder, LogTableBuilder, TxTableBuilder, TraceTableBuilder
from etha.ingest.util import trim_hash
from etha.fs import create_fs
from etha.layout import ChunkWriter


LOG = logging.getLogger(__name__)


class DataBatch(NamedTuple):
    blocks: pyarrow.Table
    transactions: pyarrow.Table
    logs: pyarrow.Table
    traces: pyarrow.Table
    bytesize: int


class BatchBuilder:
    def __init__(self):
        self._init()

    def _init(self):
        self.block_table = BlockTableBuilder()
        self.tx_table = TxTableBuilder()
        self.log_table = LogTableBuilder()
        self.trace_table = TraceTableBuilder()

    def buffered_bytes(self) -> int:
        return self.block_table.bytesize() \
            + self.tx_table.bytesize() \
            + self.log_table.bytesize() \
            + round(self.trace_table.bytesize() / 3)

    def append(self, block: Block):
        self.block_table.append(block)

        for tx in block['transactions']:
            self.tx_table.append(tx)

        for log in block.get('logs_', []):
            self.log_table.append(log)

        for trace in block.get('trace_', []):
            self.trace_table.append(trace)

    def build(self) -> DataBatch:
        bytesize = self.buffered_bytes()
        batch = DataBatch(
            blocks=self.block_table.to_table(),
            transactions=self.tx_table.to_table(),
            logs=self.log_table.to_table(),
            traces=self.trace_table.to_table(),
            bytesize=bytesize
        )
        self._init()
        return batch


class BlockWriter:
    def __init__(self, chunk_writer: ChunkWriter, with_traces: bool):
        self.chunk_writer = chunk_writer
        self.with_traces = with_traces

    def write(self, batch: DataBatch) -> None:
        blocks = batch.blocks
        transactions = batch.transactions
        logs = batch.logs
        traces = batch.traces

        block_numbers: pyarrow.ChunkedArray = blocks.column('number')
        first_block = block_numbers[0].as_py()
        last_block = block_numbers[-1].as_py()
        last_hash = trim_hash(blocks.column('hash')[-1].as_py())

        extra = {'first_block': first_block, 'last_block': last_block, 'last_hash': last_hash}
        LOG.debug('saving data chunk', extra=extra)

        transactions = transactions.sort_by([('to', 'ascending'), ('sighash', 'ascending')])
        logs = logs.sort_by([('address', 'ascending'), ('topic0', 'ascending')])

        with self.chunk_writer.write(first_block, last_block, last_hash) as loc:
            kwargs = {
                'data_page_size': 32 * 1024,
                'compression': 'zstd',
                'compression_level': 12
            }

            loc.write_parquet(
                'logs.parquet',
                logs,
                use_dictionary=['address', 'topic0'],
                row_group_size=15000,
                **kwargs
            )

            LOG.debug('wrote %s', loc.abs('logs.parquet'))

            loc.write_parquet(
                'transactions.parquet',
                transactions,
                use_dictionary=['to', 'sighash'],
                row_group_size=15000,
                **kwargs
            )

            LOG.debug('wrote %s', loc.abs('transactions.parquet'))

            if self.with_traces:
                loc.write_parquet(
                    'traces.parquet',
                    traces,
                    use_dictionary=[
                        'type',
                        'create_from',
                        'call_from',
                        'call_to',
                        'call_type',
                        'reward_author',
                        'reward_type'
                    ],
                    row_group_size=15000,
                    **kwargs
                )

                LOG.debug('wrote %s', loc.abs('traces.parquet'))

            loc.write_parquet(
                'blocks.parquet',
                blocks,
                use_dictionary=False,
                **kwargs
            )

            LOG.debug('wrote %s', loc.abs('blocks.parquet'))


class WriteOptions(NamedTuple):
    dest: str
    s3_endpoint: Optional[str] = None
    chunk_size: int = 1024
    first_block: int = 0
    last_block: Optional[int] = None
    with_traces: bool = False


# WriteOptions class is serializable, while WriteService is not
class WriteService:
    def __init__(self, options: WriteOptions):
        self.options = options

    @cache
    def chunk_writer(self) -> ChunkWriter:
        fs = create_fs(self.options.dest, s3_endpoint=self.options.s3_endpoint)
        return ChunkWriter(
            fs,
            first_block=self.options.first_block,
            last_block=self.options.last_block
        )

    @cache
    def block_writer(self) -> BlockWriter:
        return BlockWriter(
            self.chunk_writer(),
            with_traces=self.options.with_traces
        )

