from typing import Iterable

from prometheus_client import start_wsgi_server
from prometheus_client.metrics_core import Metric, GaugeMetricFamily, CounterMetricFamily
from prometheus_client.registry import REGISTRY, Collector

from sqa.util.counters import Progress
from sqa.layout import ChunkWriter


class Metrics:
    def __init__(self):
        self._metrics = set()

    def add_progress(self, progress: Progress, writer: ChunkWriter):
        self._add('progress')
        collector = _ProgressCollector(progress, writer)
        REGISTRY.register(collector)

    def serve(self, port: int):
        return start_wsgi_server(port)

    def _add(self, metric: str):
        if metric in self._metrics:
            raise Exception(f'{metric} metric was already registered')
        else:
            self._metrics.add(metric)


class _ProgressCollector(Collector):
    def __init__(self, progress: Progress, writer: ChunkWriter):
        self._progress = progress
        self._writer = writer

    def collect(self) -> Iterable[Metric]:
        progress = GaugeMetricFamily(
            'sqd_progress_blocks_per_second',
            'Overall block processing speed',
            self._progress.speed()
        )
        last_block = CounterMetricFamily(
            'sqd_last_block',
            'Last ingested block',
            self._progress.get_current_value()
        )
        last_saved_block = CounterMetricFamily(
            'sqd_last_saved_block',
            'Last saved block',
            self._writer.next_block - 1,
        )
        return [progress, last_block, last_saved_block]
