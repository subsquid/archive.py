import logging


class _Logger(logging.Logger):
    def getEffectiveLevel(self) -> int:
        from etha.log.level import get_log_level
        return get_log_level(self.name)


def init_logging():
    logging.setLoggerClass(_Logger)
    import sys
    from etha.log.format import COLORFUL, PLAIN, TextFormatter
    style = COLORFUL if sys.stderr.isatty() else PLAIN
    f = TextFormatter(style)
    h = logging.StreamHandler(sys.stderr)
    h.setFormatter(f)
    logging.basicConfig(
        handlers=[h]
    )


# def get_logger(name: str) -> logging.Logger:
#     if not isinstance(logging.getLoggerClass(), _Logger):
#         logging.setLoggerClass(_Logger)
#     return logging.getLogger(name)
