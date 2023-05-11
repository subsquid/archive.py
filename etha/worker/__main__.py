from etha.log import init_logging

init_logging()

from etha.worker.server import cli  # noqa: E402

cli()
