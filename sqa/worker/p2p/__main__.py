from sqa.util.log import init_logging

init_logging()

from sqa.worker.p2p.server import main  # noqa: E402

main()
