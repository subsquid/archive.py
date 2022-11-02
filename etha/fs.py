import datetime
import os
import urllib.parse
from contextlib import AbstractContextManager, contextmanager
from typing import Optional

import pyarrow.fs
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

    def delete(self, loc: str):
        raise NotImplementedError()


class LocalFs(Fs):
    def __init__(self, root: str):
        self._root = root

    def abs(self, *segments: str) -> str:
        return os.path.abspath(os.path.join(self._root, *segments))

    def ls(self, *segments: str) -> list[str]:
        path = self.abs(*segments)
        try:
            return os.listdir(path)
        except FileNotFoundError:
            return []

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
        path = self.abs(file)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        pyarrow.parquet.write_table(table, path, **kwargs)

    def delete(self, loc: str):
        path = self.abs(loc)
        if os.path.isdir(path):
            os.removedirs(path)
        else:
            os.remove(path)


class S3Fs(Fs):
    def __init__(self, s3: pyarrow.fs.S3FileSystem, bucket: str):
        assert bucket and bucket[0] != '/' and bucket[-1] != '/'
        self._bucket = bucket
        self._s3 = s3

    def abs(self, *segments) -> str:
        return 's3://' + self._abs_path(*segments)

    def _abs_path(self, *segments) -> str:
        path = self._bucket

        for seg in segments:
            if seg.startswith('/'):
                path = seg
            else:
                path += '/' + seg

        return self._s3.normalize_path(path)

    def ls(self, *segments: str) -> list[str]:
        path = self._abs_path(*segments)
        selector = pyarrow.fs.FileSelector(path + '/')
        return [i.base_name for i in self._s3.get_file_info(selector)]

    @contextmanager
    def transact(self, dest_dir: str) -> AbstractContextManager['Fs']:
        # Dir level transactions can't be implemented via S3 API
        path = self._abs_path(dest_dir)
        yield S3Fs(self._s3, path)

    def write_parquet(self, file: str, table, **kwargs):
        path = self._abs_path(file)
        pyarrow.parquet.write_table(table, path, filesystem=self._s3, **kwargs)

    def delete(self, loc: str):
        path = self._abs_path(loc)
        item = self._s3.get_file_info(path)
        if item.is_file:
            self._s3.delete_file(path)
        elif item.type == pyarrow.fs.FileType.Directory:
            self._s3.delete_dir(path)



def create_fs(url: str, s3_endpoint: Optional[str] = None) -> Fs:
    u = urllib.parse.urlparse(url)
    if u.scheme == 's3':
        s3 = pyarrow.fs.S3FileSystem(endpoint_override=s3_endpoint)
        bucket = u.netloc + u.path
        return S3Fs(s3, bucket)
    elif not u.scheme:
        return LocalFs(url)
    else:
        raise ValueError(f'unsupported filesystem - {url}')

