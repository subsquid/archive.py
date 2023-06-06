from typing import Iterable
import resource
from multiprocessing import active_children
import os

from etha.worker.state.manager import StateManager

from prometheus_client import generate_latest, GC_COLLECTOR, PLATFORM_COLLECTOR, PROCESS_COLLECTOR
from prometheus_client.registry import Collector, REGISTRY
from prometheus_client.metrics_core import Metric, GaugeMetricFamily



class Metrics:
    def __init__(self):
        self._metrics = set()

    def add_state(self, state: StateManager):
        self._add('state')
        collector = _StateCollector(state)
        REGISTRY.register(collector)

    def collect(self) -> bytes:
        return generate_latest()

    def _add(self, metric: str):
        if metric in self._metrics:
            raise Exception(f'{metric} metric was already registered')
        else:
            self._metrics.add(metric)


class _StateCollector(Collector):
    def __init__(self, state: StateManager):
        self._state = state

    def collect(self) -> Iterable[Metric]:
        state = GaugeMetricFamily(
            'sqd_state_available_blocks',
            'Available blocks per dataset',
            labels=['dataset'],
        )
        for dataset, ranges in self._state.get_state().items():
            blocks_sum = sum((r[1] - r[0] for r in ranges), 0)
            state.add_metric([dataset], blocks_sum)
        return [state]


class _MemoryCollector(Collector):
    def __init__(self):
        self._pagesize = resource.getpagesize()

    def collect(self) -> Iterable[Metric]:
        # code from prometheus_client was used as a reference implementation
        # https://github.com/prometheus/client_python/blob/4ab083e4dbf82e3bbb2898fc48db18998fda4d45/prometheus_client/process_collector.py#L54
        result = []
        pids = [child.pid for child in active_children()]
        pids.append(os.getpid())
        memory_usage = 0.0
        try:
            for pid in pids:
                with open(f'/proc/{pid}/stat', 'rb') as stat:
                    parts = (stat.read().split(b')')[-1].split())
                    memory_usage += float(parts[21])
        except OSError:
            pass
        else:
            rms = GaugeMetricFamily('resident_memory_bytes', 'Resident memory size in bytes',
                                    memory_usage * self._pagesize)
            result.append(rms)
        return result


REGISTRY.unregister(GC_COLLECTOR)
REGISTRY.unregister(PLATFORM_COLLECTOR)
REGISTRY.unregister(PROCESS_COLLECTOR)
REGISTRY.register(_MemoryCollector())
