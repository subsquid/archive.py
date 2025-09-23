import glob
import gzip
import json
import os
import sys
from typing import NamedTuple, Any, Iterable

from sqa.worker.query import execute_query, QueryResult, validate_query


class Fixture(NamedTuple):
    name: str
    data_dir: str
    query: Any
    result: Any


def get_fixtures(suite_dir: str) -> Iterable[Fixture]:
    data_dir = os.path.join(suite_dir, 'data')
    query_files = glob.glob('fixtures/*/query.json', root_dir=suite_dir)
    query_files.sort()
    for query_file in query_files:
        fixture_dir = os.path.join(suite_dir, os.path.dirname(query_file))
        fixture_name = os.path.basename(fixture_dir)
        query_file = os.path.join(fixture_dir, 'query.json')
        result_file = os.path.join(fixture_dir, 'result.json')

        with open(query_file) as f:
            query = json.load(f)

        with open(result_file) as f:
            result = json.load(f)

        yield Fixture(fixture_name, data_dir, query, result)


def execute_fixture_query(fixture: Fixture) -> QueryResult:
    query = validate_query(fixture.query)
    return execute_query(
        fixture.data_dir,
        (0, sys.maxsize),
        query,
        False,
        False
    )


def run_test_suite(suite_dir: str) -> None:
    suite_name = os.path.basename(suite_dir)
    for fixture in get_fixtures(suite_dir):
        print(f'test {suite_name}/{fixture.name}: ', end='')
        result = execute_fixture_query(fixture)
        result_data = json.loads(gzip.decompress(result.compressed_data))
        if result_data == fixture.result:
            print('ok')
        else:
            print('failed')
            with open(os.path.join(fixture.data_dir, '../fixtures', fixture.name, 'actual.temp.json'), 'w') as f:
                json.dump(result_data, f, indent=2)
            sys.exit(1)


def main():
    run_test_suite('tests/soneum-testnet')
    run_test_suite('tests/ethereum')
    run_test_suite('tests/binance')
    run_test_suite('tests/moonbeam')
    run_test_suite('tests/kusama')
    run_test_suite('tests/solana')
    run_test_suite('tests/tron')
    run_test_suite('tests/starknet')
    run_test_suite('tests/fuel')


if __name__ == '__main__':
    main()
