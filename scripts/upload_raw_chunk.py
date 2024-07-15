from sqa.fs import create_fs


def main():
    fs = create_fs('s3://optimism-raw')
    fs.upload(
        'data/optimism/0017561960/0017582440-0017589499-c8aa4ccc/blocks.jsonl.gz',
        '0012377060/0017582440-0017589499-c8aa4ccc/blocks.jsonl.gz'
    )


if __name__ == '__main__':
    main()
