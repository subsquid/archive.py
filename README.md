# archive.py

ArrowSquid archive tools: Python ingesters and a query worker for the SQD Network data lake.

## What it is

`archive.py` is part of [SQD](https://sqd.dev), an open data platform for Web3. It contains two kinds of components:

- **Writers / ingesters** read blocks from a chain (via an RPC node or an upstream ingest service) and write them to columnar [Apache Arrow](https://arrow.apache.org/) / Parquet files, organized into block-range chunks on local or S3-compatible storage.
- **A worker** serves range queries over those Parquet datasets. It syncs assigned data, validates incoming queries, and executes them with [DuckDB](https://duckdb.org/), exposing results over HTTP (and, optionally, a peer-to-peer transport used by [SQD Network](https://docs.sqd.dev/en/network)).

The package is published under the name `subsquid-eth-archive` and lives in the `sqa` module.

Writers are provided for several chain families, each with its own module under `sqa`:

| Chain family | Module |
|---|---|
| Ethereum / EVM | `sqa.eth.ingest` |
| Substrate | `sqa.substrate.writer` |
| Tron | `sqa.tron.writer` |
| Starknet | `sqa.starknet.writer` |
| Fuel | `sqa.fuel.writer` |
| Solana | `sqa.solana.writer` |

The worker lives in `sqa.worker` (HTTP) and `sqa.worker.p2p` (peer-to-peer).

## Disclaimer

This project is under active development, has non-obvious usage requirements, and drastic changes are expected in the near future.

We share it for the purpose of transparency and as a reference.

## Hacking

This project uses [pdm](https://pdm-project.org/latest/). If you are familiar with it, you know what to do. Otherwise, below is a recommended way to get started.

```shell
# create a virtual environment in .venv
pdm venv create /path/to/python3.11/bin/python

# install all dependencies
pdm sync -G:all
```

Use `.venv/bin/python3` to run executables and set it up as the Python interpreter for your IDE.

The `Makefile` has reference invocations for the ingesters and the worker (for example `make ingest-eth`, `make worker`, `make test`). Each writer also responds to `--help`, for example:

```shell
python3 -m sqa.eth.ingest --help
python3 -m sqa.worker --help
```

Container images are built from the multi-stage `Dockerfile` (targets such as `eth-ingest`, `solana-writer`, `substrate-writer`, `tron-writer`, `starknet-writer`, `fuel-writer`, `worker`, and `p2p-worker`).

Tests:

```shell
make test
# or
python3 -m tests.run
```

## Documentation

For SQD Network and the wider SQD stack, see [docs.sqd.dev](https://docs.sqd.dev) and [docs.sqd.dev/en/network](https://docs.sqd.dev/en/network).

## License

This project is licensed under the AGPL v3.0 license. See the [LICENSE](LICENSE.txt) file for details.
