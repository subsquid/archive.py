import sys

from .writer import Writer


def main():
    writer = Writer('data/parquet')

    for line in sys.stdin:
        if line:
            writer.append(line)

    writer.flush()


if __name__ == '__main__':
    main()

