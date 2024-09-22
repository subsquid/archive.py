from sqa.fs import create_fs
from sqa.layout import get_chunks
import sys


def main():
    # 2048 - default chunk size
    bucket = sys.argv[1]
    fs = create_fs(bucket)
    invalid = []
    expected = 0
    for chunk in get_chunks(fs):
        if chunk.first_block != expected:
            invalid.append([expected, chunk.first_block - 1])
        expected = chunk.last_block + 1
    print(invalid)


if __name__ == '__main__':
    main()
