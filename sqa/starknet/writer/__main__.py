from sqa.util.log import init_logging

init_logging()

from .main import main  # noqa: E402

main(__name__)