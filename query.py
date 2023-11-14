import json
import sys

from sqa.query.schema import ArchiveQuery
from sqa.worker.query import execute_query


def main():
    dataset_dir = sys.argv[1]
    query_file = sys.argv[2]

    with open(query_file) as f:
        q: ArchiveQuery = json.load(f)

    result = execute_query(dataset_dir, (0, sys.maxsize), q)

    json.dump(json.loads(result.result), sys.stdout, indent=2)


if __name__ == '__main__':
    main()
