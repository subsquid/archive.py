from sqa.fs import create_fs
from sqa.layout import get_chunks


def main():
    # 2048 - default chunk size
    fs = create_fs('s3://optimism-raw')
    expected = 0
    for chunk in get_chunks(fs):
        print(chunk.first_block, expected)
        assert chunk.first_block == expected
        expected = chunk.last_block + 1


if __name__ == '__main__':
    main()
