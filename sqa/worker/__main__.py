from sqa.util.log import init_logging

init_logging()

from sqa.worker.server import cli  # noqa: E402

cli()
