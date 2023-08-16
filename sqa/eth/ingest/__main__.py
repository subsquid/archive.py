from sqa.util.log import init_logging

init_logging()

from sqa.eth.ingest.main import cli  # noqa: E402

cli()
