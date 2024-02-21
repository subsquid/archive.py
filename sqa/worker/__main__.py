from sqa.init import init_logging, init_uvloop

init_logging()
init_uvloop()

from sqa.worker.server import cli  # noqa: E402

cli()
