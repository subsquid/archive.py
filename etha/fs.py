import os
import shutil
import tempfile
import urllib.parse
from contextlib import AbstractContextManager, contextmanager
from typing import Optional, IO, TypedDict

import pyarrow.fs
import pyarrow.parquet
import s3fs

from etha.util import add_temp_prefix


class Fs:
    def abs(self, *segments: str) -> str:
        raise NotImplementedError()

    def ls(self, *segments: str) -> list[str]:
        raise NotImplementedError()

    def transact(self, dest_dir: str) -> AbstractContextManager['Fs']:
        raise NotImplementedError()

    def open(self, loc: str, mode: str) -> IO:
        raise NotImplementedError()

    def delete(self, loc: str):
        raise NotImplementedError()

    def download(self, src_loc: str, local_dest: str):
        raise NotImplementedError()

    def upload(self, local_src: str, dest: str):
        raise NotImplementedError()

    def write_parquet(self, name: str, table, **kwargs):
        raise NotImplementedError()

    def cd(self, *segments) -> 'Fs':
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
        temp_dir = add_temp_prefix(path)
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
            temp_dir = add_temp_prefix(path)
            os.rename(path, temp_dir)
            shutil.rmtree(temp_dir)
        else:
            try:
                os.remove(path)
            except FileNotFoundError:
                pass

    def cd(self, *segments) -> 'LocalFs':
        return LocalFs(self.abs(*segments))

    def open(self, loc: str, mode: str) -> IO:
        path = self.abs(loc)
        return open(path, mode)


class _S3File(TypedDict):
    path: str
    size: int
    etag: str


class S3DownloadException(Exception):
    pass


class S3Fs(Fs):
    def __init__(self, s3: s3fs.S3FileSystem, bucket: str):
        assert bucket and bucket[0] != '/' and bucket[-1] != '/'
        self._bucket = bucket
        self._s3 = s3

    def abs(self, *segments) -> str:
        return 's3://' + self._abs_path(*segments)

    def _abs_path(self, *paths: str) -> str:
        segments = [self._bucket]

        for path in paths:
            if path.startswith('/'):
                segments = []
                path = path[1:]

            for seg in path.split('/'):
                assert seg
                if seg == '.':
                    pass
                elif seg == '..':
                    segments.pop()
                else:
                    segments.append(seg)

        assert segments
        return '/'.join(segments)

    def ls(self, *segments: str) -> list[str]:
        path = self._abs_path(*segments)
        return [os.path.basename(i) for i in self._s3.ls(path, detail=False)]

    @contextmanager
    def transact(self, dest_dir: str) -> AbstractContextManager['Fs']:
        # Dir level transactions can't be implemented via S3 API
        path = self._abs_path(dest_dir)
        yield S3Fs(self._s3, path)

    def delete(self, loc: str):
        path = self._abs_path(loc)
        self._s3.delete(path, recursive=True)

    def download(self, src_loc: str, local_dest: str):
        src_path = self._abs_path(src_loc)
        src_files = self._list_files(src_path)
        self._s3.download(src_path, local_dest, recursive=True)
        # Once in a while s3fs manages to successfully download incomplete file.
        # As a dirty workaround we are checking downloaded files below
        for src_file in src_files:
            dest = os.path.join(local_dest, os.path.relpath(src_file['path'], src_path))
            try:
                stat = os.stat(dest)
            except IOError as reason:
                raise S3DownloadException(f'failed to stat downloaded file') from reason
            if stat.st_size != src_file['size']:
                raise S3DownloadException(f's3://{src_file["path"]} download to {dest} was incomplete')

    def _list_files(self, path: str) -> list[_S3File]:
        result: list[_S3File] = []
        for loc, dirs, files in self._s3.walk(path, detail=True):
            for filename, stat in files.items():
                result.append({
                    'path': f'{loc}/{filename}'if filename else loc,
                    'size': stat['Size'],
                    'etag': stat['ETag']
                })
        return result

    def upload(self, local_src: str, dest: str):
        self._s3.upload(local_src, self._abs_path(dest), recursive=True)

    def write_parquet(self, dest: str, table, **kwargs):
        # Save via temporary local file to work around - https://github.com/fsspec/s3fs/issues/749
        tmp = tempfile.NamedTemporaryFile(delete=False)
        try:
            with tmp:
                pyarrow.parquet.write_table(table, tmp, **kwargs)
            self.upload(tmp.name, dest)
        finally:
            os.remove(tmp.name)

    def open(self, loc: str, mode: str) -> IO:
        path = self._abs_path(loc)
        return self._s3.open(path, mode)

    def cd(self, *segments) -> 'S3Fs':
        return S3Fs(self._s3, self._abs_path(*segments))


def create_fs(url: str, s3_endpoint: Optional[str] = os.environ.get('AWS_S3_ENDPOINT')) -> Fs:
    u = urllib.parse.urlparse(url)
    if u.scheme == 's3':
        client_kwargs = {}
        if s3_endpoint:
            client_kwargs['endpoint_url'] = s3_endpoint
        s3 = s3fs.S3FileSystem(client_kwargs=client_kwargs, use_listings_cache=False)
        bucket = u.netloc + u.path
        return S3Fs(s3, bucket)
    elif not u.scheme:
        return LocalFs(url)
    else:
        raise ValueError(f'unsupported filesystem - {url}')

