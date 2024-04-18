from sqa.writer import Writer
from sqa.writer.cli import CLI
from .parquet import ParquetWriter


class _CLI(CLI):
    validate_chain_continuity = False

    def create_writer(self) -> Writer:
        return ParquetWriter()


def main(module_name: str) -> None:
    _CLI(module_name).main()
