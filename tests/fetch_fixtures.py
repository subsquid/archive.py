import glob
import json
import math
import os.path
import sys

import requests

from sqa.fs import LocalFs
from sqa.layout import get_chunks
from sqa.query.schema import ArchiveQuery


def fetch_suite(suite_dir: str, archive_url: str) -> None:
    data_dir = os.path.join(suite_dir, 'data')
    chunk = next(iter(get_chunks(LocalFs(data_dir))))

    query_files = glob.glob('fixtures/*/query.json', root_dir=suite_dir)
    query_files.sort()

    for query_file in query_files:
        fixture_dir = os.path.join(suite_dir, os.path.dirname(query_file))
        fixture_name = os.path.basename(fixture_dir)
        query_file = os.path.join(fixture_dir, 'query.json')
        result_file = os.path.join(fixture_dir, 'result.json')

        print(f'fetching {fixture_name}')

        with open(query_file) as f:
            q: ArchiveQuery = json.load(f)
            q['fromBlock'] = max(chunk.first_block, q['fromBlock'])
            q['toBlock'] = min(chunk.last_block, q.get('toBlock', math.inf))

        with open(query_file, 'w') as f:
           json.dump(q, f, indent='  ')

        res = requests.get(f'{archive_url}/{chunk.first_block}/worker')
        worker_url = res.text

        res = requests.post(worker_url, json=q)
        result_data = res.json()

        with open(result_file, 'w') as f:
            json.dump(result_data, f, indent=2)


def main():
    suite_dir = sys.argv[1]
    archive_url = sys.argv[2]
    fetch_suite(suite_dir, archive_url)


if __name__ == '__main__':
    main()
