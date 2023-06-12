# archive.py

Stage 2 subsquid archive tools for ethereum compatible chains.

## Disclaimer

This project is under active development, has non-obvious usage requirements, 
drastic changes are expected in the near future.

We share it for the purpose of transparency and as a reference.

## Hacking

This project uses [pdm(1)](https://pdm.fming.dev/latest/). 
If you are familiar with it - you know what to do. 
Otherwise, below is a recommended way to get started.

```shell
# create a virtual environment in .venv
pdm venv create /path/to/python3.11/bin/python

# install all dependencies
pdm sync -G:all
```

Use `.env/bin/python3` to run executables and set it up as a python interpreter for your IDE.
