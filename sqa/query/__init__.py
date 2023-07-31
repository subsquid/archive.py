import os

from sqa.layout import DataChunk


class SqlQuery:
    def __init__(
            self,
            sql: str,
            params: list,
            variables: dict[str, int],
            tables: list[str],
            last_block: int | None = None
    ):
        self.sql = sql
        self.params = params
        self._variables = variables
        self._tables = list(t for t in tables if t in variables)
        self._last_block = last_block

    def set_chunk(self, dataset_dir: str, chunk: DataChunk) -> None:
        for table in self._tables:
            idx = self._variables[table]
            self.params[idx] = os.path.join(dataset_dir, chunk.path(), f'{table}.parquet')

        if 'last_block' in self._variables:
            idx = self._variables['last_block']
            if self._last_block is None:
                self.params[idx] = chunk.last_block
            else:
                self.params[idx] = min(chunk.last_block, self._last_block)
