import logging
import traceback
from datetime import datetime
from io import StringIO
from typing import NamedTuple
import json


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


def _print_exception(exc_info) -> str:
    sio = StringIO()
    traceback.print_exception(exc_info[0], exc_info[1], exc_info[2], None, sio)
    s = sio.getvalue()
    sio.close()
    if s[-1:] == '\n':
        s = s[:-1]
    return s


class StructFormatter:
    def format(self, rec: logging.LogRecord) -> str:
        obj = {
            'time': int(rec.created * 1000),
            'level': rec.levelname,
            'ns': rec.name,
            'msg': rec.getMessage(),
        }

        for k in rec.__dict__:
            if k not in known_attributes:
                obj[k] = rec.__dict__[k]

        if rec.exc_info:
            obj['err'] = _print_exception(rec.exc_info)

        return json.dumps(obj)
