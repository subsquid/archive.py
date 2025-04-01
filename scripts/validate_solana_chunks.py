from sqa.fs import create_fs
from sqa.layout import get_chunks, get_tops
from concurrent.futures import ThreadPoolExecutor
import concurrent.futures


def check_chunk(fs, chunk):
    table = fs.read_parquet('blocks.parquet')
    first_parent_number = table['parent_number'][0].as_py()
    return first_parent_number + 1 == chunk.first_block


def main():
    fs = create_fs('s3://solana-mainnet-3')
    with ThreadPoolExecutor(max_workers=20) as executor:
        futures = []
        for chunk in get_chunks(fs, 310300193, 328297241):
            print('processing', chunk.path())
            chunk_fs = fs.cd(chunk.path())
            future = executor.submit(check_chunk, chunk_fs, chunk)
            futures.append(future)
            if len(futures) == 20:
                for future in concurrent.futures.as_completed(futures):
                    assert future.result()
                futures.clear()


if __name__ == '__main__':
    main()
