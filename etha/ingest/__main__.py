from etha.log import init_logging

init_logging()

from etha.ingest.main import cli  # noqa: E402

cli()
