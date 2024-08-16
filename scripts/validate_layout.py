from sqa.fs import create_fs
from sqa.layout import get_chunks


invalid = [
    # [30782720, 30830239],
    # [33968100, 34021919],
    # [37706980, 37762359],
    [41995720, 42042599], 
    [47389700, 47464619], 
    [52977600, 53028859], 
    [59206220, 59241259], 
    [63964560, 64024219], 
    [69125280, 69147679], 
    [71996520, 72017919], 
    [75040480, 75059859], 
    [77996020, 78027719], 
    [81471800, 81504739], 
    [84967300, 85027999], 
    [89196040, 89226259], 
    [92992200, 93021679], 
    [96664300, 96689919], 
    [99985840, 100017579], 
    [104715380, 104750679]
]


def main():
    # 2048 - default chunk size
    fs = create_fs('s3://optimism-raw')
    # invalid = []
    expected = 0
    for chunk in get_chunks(fs):
        print(chunk.first_block, expected)
        msg = f'reindex from {expected} to {chunk.first_block - 1}'
        if chunk.first_block != expected:
            invalid.append([expected, chunk.first_block - 1])
        assert chunk.first_block == expected, msg
        expected = chunk.last_block + 1
    # print(invalid)


if __name__ == '__main__':
    main()
