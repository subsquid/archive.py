from sqa.fs import create_fs
from sqa.layout import get_chunks

def main():
    fs = create_fs('s3://solana-mainnet-0')
    corrupted = []
    for chunk in get_chunks(fs, 225545600):
        # print(f'processing {chunk.path()}')
        filelist = fs.ls(chunk.path())
        if len(filelist) != 7:
            print(chunk.path())
            corrupted.append(chunk.path())
    print(corrupted)


if __name__ == '__main__':
    main()
