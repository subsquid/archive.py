import base64
import datetime
import json
import logging
import urllib.parse

from locust import FastHttpUser, task

from etha.query.model import Query

LOG = logging.getLogger(__name__)


class _WorkerUser(FastHttpUser):
    def __init__(self, user_name: str, dataset: str, query: Query, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.user_name = user_name
        self.dataset = dataset
        self.query = query
        self.last_block = query['toBlock']

    @task
    def sync(self):
        ds = base64.urlsafe_b64encode(self.dataset.encode('utf-8')).decode('ascii')
        q: Query = dict(self.query)
        beg = datetime.datetime.now()
        while q['fromBlock'] <= self.last_block:
            res = self.client.post(f'/query/{ds}', json=q)
            next_block = int(res.headers['x-sqd-last-processed-block']) + 1
            assert next_block > q['fromBlock']
            q['fromBlock'] = next_block
        dur = datetime.datetime.now() - beg
        LOG.info(f'{self.user_name} synced in {round(dur.total_seconds())} seconds')


class _ArchiveUser(FastHttpUser):
    def __init__(self, user_name: str, dataset: str, query: Query, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.user_name = user_name
        self.dataset = dataset
        self.query = query
        self.first_block = query['fromBlock']
        self.last_block = query['toBlock']

    @task
    def sync(self):
        ds_b64 = base64.urlsafe_b64encode(self.dataset.encode('utf-8')).decode('ascii')
        ds_uri = urllib.parse.quote(self.dataset, safe='')
        q: Query = dict(self.query)
        beg = datetime.datetime.now()
        while q['fromBlock'] <= self.last_block:
            res = self.client.get(f'/worker/{ds_uri}/{q["fromBlock"]}', name='/router')
            res.raise_for_status()
            worker = json.loads(res.text)
            res = self.client.post(f'{worker}/query/{ds_b64}', json=q, name=worker)
            res.raise_for_status()
            next_block = int(res.headers['x-sqd-last-processed-block']) + 1
            assert next_block > q['fromBlock']
            q['fromBlock'] = next_block
        dur = datetime.datetime.now() - beg
        LOG.info(f'{self.user_name}({self.first_block}, {self.last_block}) synced in {round(dur.total_seconds())} seconds')


def WorkerUser(name: str, dataset: str, query: Query):
    def __init__(self, *args, **kwargs):
        _WorkerUser.__init__(self, name, dataset, query, *args, **kwargs)

    return type(name, (_WorkerUser,), {'__init__': __init__})


def ArchiveUser(name: str, dataset: str, query: Query):
    def __init__(self, *args, **kwargs):
        _ArchiveUser.__init__(self, name, dataset, query, *args, **kwargs)

    return type(name, (_ArchiveUser,), {'__init__': __init__})
