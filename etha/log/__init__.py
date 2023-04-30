import logging


class _Logger(logging.Logger):
    def getEffectiveLevel(self) -> int:
        from etha.log.level import get_log_level
        return get_log_level(self.name)


def init_logging():
    logging.setLoggerClass(_Logger)
    import sys
    from etha.log.format import COLORFUL, PLAIN, TextFormatter, StructFormatter
    f = TextFormatter(COLORFUL) if sys.stderr.isatty() else StructFormatter()
    h = logging.StreamHandler(sys.stderr)
    h.setFormatter(f)
    logging.basicConfig(
        handlers=[h]
    )
