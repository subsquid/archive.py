import os.path
import datetime
from contextlib import AbstractContextManager, contextmanager

import pyarrow.filesystem
import pyarrow.parquet


class Fs:
    def abs(self, *segments: str) -> str:
        raise NotImplementedError()

    def ls(self, *segments: str) -> list[str]:
        raise NotImplementedError()

    def transact(self, dest_dir: str) -> AbstractContextManager['Fs']:
        raise NotImplementedError

    def write_parquet(self, name: str, table, **kwargs):
        raise NotImplementedError()


class LocalFs(Fs):
    def __init__(self, root: str):
        self._root = root

    def abs(self, *segments: str) -> str:
        return os.path.join(self._root, *segments)

    def ls(self, *segments: str) -> list[str]:
        path = self.abs(*segments)
        return os.listdir(path)

    @contextmanager
    def transact(self, dest_dir: str) -> AbstractContextManager['LocalFs']:
        path = self.abs(dest_dir)
        basename = os.path.basename(path)
        parent = os.path.dirname(path)
        ts = round(datetime.datetime.now().timestamp() * 1000)
        temp_dir = os.path.join(parent, f'temp-{ts}-{basename}')
        yield LocalFs(temp_dir)
        if os.path.exists(temp_dir):
            os.rename(temp_dir, path)

    def write_parquet(self, file: str, table, **kwargs):
        pyarrow.parquet.write_table(table, self.abs(file), **kwargs)
