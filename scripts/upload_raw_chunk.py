from sqa.fs import create_fs


def main():
    fs = create_fs('s3://optimism-raw')
    chunk = '0041995720-0042042599-bc71e959'
    fs.upload(
        f'data/optimism/0041995720/{chunk}/blocks.jsonl.gz',
        f'0037762360/{chunk}/blocks.jsonl.gz'
    )


if __name__ == '__main__':
    main()
