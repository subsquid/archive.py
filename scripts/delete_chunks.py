from sqa.fs import create_fs
from sqa.layout import get_chunks

def main():
    fs = create_fs('s3://tron-mainnet-raw')
    for chunk in get_chunks(fs, 65399115):
        fs.delete(chunk.path())


if __name__ == '__main__':
    main()
