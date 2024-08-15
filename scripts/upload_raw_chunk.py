from sqa.fs import create_fs


def main():
    fs = create_fs('s3://optimism-raw')
    chunk = '0037743990-0037762359-bc27caed'
    fs.upload(
        f'data/optimism/0037743990/{chunk}/blocks.jsonl.gz',
        f'0030830240/{chunk}/blocks.jsonl.gz'
    )


if __name__ == '__main__':
    main()
