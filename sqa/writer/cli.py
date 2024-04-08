import argparse
import os
from functools import cache
from typing import Iterable

from . import Writer, Sink, Block
from .ingest import ingest_from_service, ingest_from_stdin


class CLI:
    def __init__(self, module_name: str):
        self.module_name = module_name

    def create_writer(self) -> Writer:
        raise NotImplementedError()

    def get_default_chunk_size(self) -> int:
        return 1024

    def get_default_top_dir_size(self) -> int:
        return 500

    @cache
    def _arguments(self):
        program = argparse.ArgumentParser(
            prog=f'python3 -m {self.module_name}',
            description='Subsquid substrate archive writer'
        )

        program.add_argument(
            'dest',
            metavar='ARCHIVE',
            help='target dir or s3 location to write data to'
        )

        program.add_argument(
            '-s', '--src',
            type=str,
            metavar='URL',
            help='URL of the data ingestion service'
        )

        program.add_argument(
            '--first-block',
            type=int,
            default=0,
            metavar='N',
            help='first block of a range to write'
        )

        program.add_argument(
            '--last-block',
            type=int,
            metavar='N',
            help='last block of a range to write'
        )

        program.add_argument(
            '--chunk-size',
            metavar='MB',
            type=int,
            default=self.get_default_chunk_size(),
            help='data chunk size in roughly estimated megabytes'
        )

        program.add_argument(
            '--top-dir-size',
            metavar='MB',
            type=int,
            default=self.get_default_top_dir_size(),
            help='number of chunks in top-level dir'
        )

        program.add_argument(
            '--get-next-block',
            action='store_true',
            help='check the stored data, print the next block to write and exit'
        )

        program.add_argument(
            '--prom-port',
            type=int,
            help='port to use for built-in prometheus metrics server'
        )

        return program.parse_args()

    @cache
    def _writer(self) -> Writer:
        return self.create_writer()

    @cache
    def _sink(self) -> Sink:
        args = self._arguments()
        return Sink(
            self._writer(),
            dest=args.dest,
            first_block=args.first_block,
            last_block=args.last_block,
            chunk_size=args.chunk_size
        )

    def _start_prometheus_metrics(self):
        port = self._arguments().prom_port
        if port is None:
            return

        from prometheus_client import REGISTRY, start_wsgi_server
        from .metrics import SinkMetricsCollector
        REGISTRY.register(SinkMetricsCollector(self._sink()))
        start_wsgi_server(port)

    def _ingest(self) -> Iterable[list[Block]]:
        args = self._arguments()
        writer = self._writer()
        first_block = self._sink().get_next_block()
        last_block = args.last_block

        if last_block is not None and first_block > last_block:
            return

        if args.src:
            blocks = ingest_from_service(
                args.src,
                writer.get_block_height,
                first_block,
                last_block
            )
        else:
            blocks = ingest_from_stdin(
                writer.get_block_height,
                first_block,
                last_block
            )

        pack = []
        for b in blocks:
            pack.append(b)
            if len(pack) >= 50:
                yield pack
                pack = []
        if pack:
            yield pack

    def init_support_services(self):
        self._start_prometheus_metrics()

        if os.getenv('SENTRY_DSN'):
            import sentry_sdk
            sentry_sdk.init(
                traces_sample_rate=1.0
            )

    def main(self) -> None:
        self.init_support_services()
        sink = self._sink()
        sink.write(self._ingest())
