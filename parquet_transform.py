import glob
import os.path

import pyarrow
import pyarrow.parquet

from etha.util import add_temp_prefix


def transform(mapper, data_dir: str) -> None:
    for temp_item in glob.glob('**/temp-*.parquet', root_dir=data_dir, recursive=True):
        os.unlink(os.path.join(data_dir, temp_item))

    items = glob.glob('**/*.parquet', root_dir=data_dir, recursive=True)
    items.sort()
    for item in items:
        bn = os.path.basename(item)
        kind = os.path.splitext(bn)[0]
        write = getattr(mapper, kind, None)
        if write:
            file = os.path.join(data_dir, item)
            table = pyarrow.parquet.read_table(file)
            temp = add_temp_prefix(file)
            write(table, temp)
            if os.path.exists(temp):
                os.unlink(file)
                os.rename(temp, file)
                print(f'updated {item}')


class Mapper:
    def blocks(self, table: pyarrow.Table, out: str):
        self._write(table, out, use_dictionary=False)

    def logs(self, table: pyarrow.Table, out: str):
        table = table.sort_by([
            ('address', 'ascending'),
            ('topic0', 'ascending'),

        ])
        self._write(
            table,
            out,
            use_dictionary=['address', 'topic0'],
            row_group_size=15000
        )

    def transactions(self, table: pyarrow.Table, out: str):
        table = table.sort_by([
            ('to', 'ascending'),
            ('sighash', 'ascending')
        ])
        self._write(
            table,
            out,
            use_dictionary=['to', 'sighash'],
            row_group_size=15000
        )

    def _write(self, table: pyarrow.Table, out: str, **kwargs):
        pyarrow.parquet.write_table(
            table,
            out,
            data_page_size=32*1024,
            compression='zstd',
            compression_level=16,
            **kwargs
        )


if __name__ == '__main__':
    transform(Mapper(), 'data/worker')
