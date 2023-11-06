from sqa.util.log import init_logging

init_logging()

from .main import cli  # noqa: E402

cli()
