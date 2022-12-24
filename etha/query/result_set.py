from io import BytesIO
from typing import Optional

import pyarrow


class ResultSet:
    _inputs: dict[str, pyarrow.RecordBatchStreamWriter]
    _outputs: dict[str, 'BytesBuffer']

    def __init__(self):
        self._inputs = {}
        self._outputs = {}
        self.size = 0

    def write_blocks(self, table: Optional[pyarrow.Table]):
        self._write('blocks', table)

    def write_transactions(self, table: Optional[pyarrow.Table]):
        self._write('transactions', table)

    def write_logs(self, table: Optional[pyarrow.Table]):
        self._write('logs', table)

    def _write(self, name: str, table: Optional[pyarrow.Table]):
        if not table or table.shape[0] == 0:
            return

        inp = self._inputs.get(name)
        if not inp:
            out = BytesBuffer()
            inp = pyarrow.ipc.new_stream(pyarrow.CompressedOutputStream(out, 'zstd'), table.schema)
            self._inputs[name] = inp
            self._outputs[name] = out

        inp.write_table(table)
        self.size += table.get_total_buffer_size()

    def close(self) -> dict[str, bytes]:
        tables = {}
        for table, inp in self._inputs.items():
            inp.close()
            out = self._outputs[table]
            tables[table] = out.getvalue()
        return tables


class BytesBuffer(BytesIO):
    def close(self):
        pass
