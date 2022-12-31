import base64
import urllib.parse


Dataset = str


def dataset_encode(ds: Dataset) -> str:
    return base64.urlsafe_b64encode(ds.encode('utf-8')).decode('ascii')


def dataset_decode(s: str) -> Dataset:
    ds = base64.urlsafe_b64decode(s).decode(encoding='utf-8')
    urllib.parse.urlparse(ds)
    return ds
