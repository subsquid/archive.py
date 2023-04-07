import logging
import sys
import traceback
import os
import re
from datetime import datetime
from io import StringIO
from typing import Any, NamedTuple


class Style(NamedTuple):
    reset: str
    dim: str
    log_name: str
    level_debug: str
    level_info: str
    level_warn: str
    level_error: str
    level_critical: str
    level_unknown: str


COLORFUL = Style(
    reset='\033[0m',
    dim='\033[2m',
    log_name='\033[1m\033[34m',
    level_debug='\033[1m\033[32m',
    level_info='\033[1m\033[36m',
    level_warn='\033[1m\033[33m',
    level_error='\033[1m\033[31m',
    level_critical='\033[1m\033[31m',
    level_unknown='\033[1m'
)


PLAIN = Style(
    reset='',
    dim='',
    log_name='',
    level_debug='',
    level_info='',
    level_warn='',
    level_error='',
    level_critical='',
    level_unknown=''
)


known_attributes = set(
    logging.LogRecord(
        name='dummy',
        level=logging.INFO,
        pathname='',
        lineno=1,
        msg='',
        args=(),
        exc_info=None
    ).__dict__.keys()
)


known_attributes.add('color_message')


class TextFormatter:
    def __init__(self, style: Style):
        self.style = style
        self.levels = {
            logging.DEBUG: f'{style.level_debug}DEBUG{style.reset}',
            logging.INFO: f'{style.level_info}INFO{style.reset} ',
            logging.WARNING: f'{style.level_warn}WARN{style.reset} ',
            logging.ERROR: f'{style.level_error}ERROR{style.reset}',
            logging.CRITICAL: f'{style.level_critical}FATAL{style.reset}'
        }

    def format(self, rec: logging.LogRecord) -> str:
        s = self.style

        time = datetime.fromtimestamp(rec.created).strftime('%H:%M:%S')

        level = self.levels.get(rec.levelno)
        if level is None:
            level = s.level_unknown + f'L{rec.levelno}'.ljust(5, ' ') + s.reset

        kvl = []
        for k in rec.__dict__:
            if k not in known_attributes:
                kvl.append(f' {k}={repr(rec.__dict__[k])}')

        if kvl:
            kvs = f'{s.dim}{"".join(kvl)}{s.reset}'
        else:
            kvs = ''

        if rec.exc_info:
            exc_info = f'\n\n{s.dim}{_print_exception(rec.exc_info)}{s.reset}\n'
        else:
            exc_info = ''

        return f'{time} {level} {s.log_name}{rec.name}{s.reset} {rec.getMessage()}{kvs}{exc_info}'


def compile_level_config(config: str):
    variants = []
    for ns in config.split(','):
        ns = ns.strip()
        pattern = f'^{"(.*)".join(ns.split("*"))}(:.*)?$'
        regex = re.compile(pattern)

        def match(ns: str):
            m = regex.match(ns)
            if m is None:
                return 0

            specificity = len(ns) + 1
            for group in m.groups():
                if group is not None:
                    specificity -= len(group)
            return specificity

        variants.append(match)

    def matcher(ns: str):
        specificity = 0
        for variant in variants:
            specificity = max(specificity, variant(ns))
        return specificity

    return matcher


def no_match(ns: str) -> int:
    return 0


class NamespaceFilter(logging.Filter):
    levels = [no_match, no_match, no_match, no_match, no_match, no_match]

    def __init__(self, root_ns: str | None):
        self.root_ns = root_ns

    def configure(self, level: int, config: str):
        self.levels[level] = compile_level_config(config)

    def ns_level(self, ns: str) -> int | None:
        specificity = 0
        level = logging.INFO if self.is_root_ns(ns) else None
        for i, matcher in enumerate(self.levels):
            s = matcher(ns)
            if s > specificity:
                level = i * 10
                specificity = s
        return level

    def is_root_ns(self, ns: str):
        return self.root_ns is not None and self.root_ns in ns

    def filter(self, record: logging.LogRecord) -> bool:
        if level := self.ns_level(record.name):
            return record.levelno >= level
        return False


def _print_exception(exc_info) -> str:
    sio = StringIO()
    traceback.print_exception(exc_info[0], exc_info[1], exc_info[2], None, sio)
    s = sio.getvalue()
    sio.close()
    if s[-1:] == '\n':
        s = s[:-1]
    return s


def init_logging(root_ns: str | None):
    ns_filter = NamespaceFilter(root_ns)
    min_level = None

    levels = ['TRACE', 'DEBUG', 'INFO', 'WARN', 'ERROR', 'FATAL']
    for level, name in enumerate(levels):
        if env := os.getenv(f'SQD_{name}'):
            ns_filter.configure(level, env)
            if min_level is None:
                min_level = level * 10

    if min_level is None:
        min_level = logging.INFO

    style = COLORFUL if sys.stderr.isatty() else PLAIN
    f: Any = TextFormatter(style)
    h = logging.StreamHandler(sys.stderr)
    h.setFormatter(f)
    h.addFilter(ns_filter)
    logging.basicConfig(
        level=min_level,
        handlers=[h]
    )
