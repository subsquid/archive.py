import logging
import os
import re
from functools import cache
from typing import Callable


def _compile_level_config(config: str) -> Callable[[str], int]:
    variants = []
    for ns in config.split(','):
        ns = ns.strip()
        pattern = f'^{"(.*)".join(ns.split("*"))}(\\..*)?$'
        regex = re.compile(pattern)

        def match_variant(ns: str):
            m = regex.match(ns)
            if m is None:
                return 0

            specificity = len(ns) + 1
            for group in m.groups():
                if group is not None:
                    specificity -= len(group)
            return specificity

        variants.append(match_variant)

    def match_level(ns: str) -> int:
        specificity = 0
        for variant in variants:
            specificity = max(specificity, variant(ns))
        return specificity

    return match_level


def _no_match(ns: str) -> int:
    return 0


@cache
def _get_matchers():
    matchers = []
    for level in ['TRACE', 'DEBUG', 'INFO', 'WARN', 'ERROR', 'FATAL']:
        if env := os.getenv(f'SQD_{level}'):
            matcher = _compile_level_config(env)
        else:
            matcher = _no_match
        matchers.append(matcher)
    return matchers


def get_log_level(ns: str) -> int:
    level = logging.INFO
    specificity = 0
    matchers = _get_matchers()
    for index, matcher in enumerate(matchers):
        s = matcher(ns)
        if s > specificity:
            level = index * 10
            specificity = s
    return level
