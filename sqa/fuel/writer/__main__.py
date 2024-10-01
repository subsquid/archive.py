from sqa.util.log import init_logging

init_logging()

from .cli import main  # noqa: E402

main(__name__)
