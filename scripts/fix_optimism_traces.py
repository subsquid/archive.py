import subprocess
from sqa.fs import create_fs
from sqa.layout import get_tops
import sys


def main():
    invalid = [
        [41995720, 42042599],
        # [47389700, 47464619],
        # [52977600, 53028859],
        # [59206220, 59241259],
        # [63964560, 64024219],
        # [69125280, 69147679],
        # [71996520, 72017919],
        # [75040480, 75059859],
        # [77996020, 78027719],
        # [81471800, 81504739],
        # [84967300, 85027999],
        # [89196040, 89226259],
        # [92992200, 93021679],
        # [96664300, 96689919],
        # [99985840, 100017579],
        # [104715380, 104750679]
    ]
    fs = create_fs('s3://optimism-raw')
    tops = get_tops(fs)
    for (first_block, last_block) in invalid:
        subprocess.run([
            'python3', '-m', 'sqa.eth.ingest', '--dest', 'data/optimism',
                '--raw',
                '--endpoint', 'https://optimism-mainnet.blastapi.io/a9b69b53-4921-4137-8abb-37425c1e8968',
                '--first-block', str(first_block),
                '--last-block', str(last_block),
                '--with-receipts',
                '--with-traces',
                '--write-chunk-size', '1024'
        ], check=True, stdout=sys.stdout, stderr=subprocess.STDOUT)
        top = None
        for t in tops:
            if top > first_block:
                break
            top = t
        print(top)



if __name__ == '__main__':
    main()
