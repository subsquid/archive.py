from sqa.init import init_logging, init_uvloop

init_logging()
init_uvloop()

from sqa.worker.p2p.server import main  # noqa: E402

main()
