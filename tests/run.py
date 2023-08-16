import os
import glob
import json
import sys
from typing import NamedTuple, Any

from sqa.worker import query


class Fixture(NamedTuple):
    name: str
    query: Any
    result: Any


def get_fixtures():
    for path in glob.glob('tests/fixtures/*.result.json'):
        name = os.path.basename(path).split('.')[0]
        query_path = path.replace('.result', '')
        with open(query_path) as query_file, open(path) as file:
            query = json.load(query_file)
            result = json.load(file)
            yield Fixture(name, query, result)


def execute_query(q: query.ArchiveQuery):
    res = query.execute_query('tests/data/', (17881390, 17882786), q)
    return json.loads(res.result)


def run_test(fixture: Fixture) -> bool:
    actual_result = execute_query(fixture.query)
    return actual_result == fixture.result


def main():
    for fixture in get_fixtures():
        if run_test(fixture):
            print(f'test "{fixture.name}" successfully passed')
        else:
            print(f'test "{fixture.name}" failed')
            with open(f'{fixture.name}.actual.temp.json', 'w') as f:
                json.dump(execute_query(fixture.query), f, indent=4)
            sys.exit(1)


if __name__ == '__main__':
    main()
