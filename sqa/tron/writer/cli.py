from sqa.writer import Writer
from sqa.writer.cli import CLI
from .parquet import ParquetWriter


class _CLI(CLI):
    def create_writer(self) -> Writer:
        return ParquetWriter()

    def get_default_chunk_size(self) -> int:
        return 2048


def main(module_name: str) -> None:
    _CLI(module_name).main()
