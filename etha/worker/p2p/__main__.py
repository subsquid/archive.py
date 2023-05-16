from etha.log import init_logging

init_logging()

from etha.worker.p2p.server import main  # noqa: E402

main()
