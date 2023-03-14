from typing import Iterable

from prometheus_client import start_wsgi_server
from prometheus_client.metrics_core import Metric, GaugeMetricFamily, CounterMetricFamily
from prometheus_client.registry import REGISTRY, Collector

from etha.counters import Progress
from etha.ingest.rpc import RpcClient


class Metrics:
    def __init__(self):
        self._metrics = set()

    def add_progress(self, progress: Progress):
        self._add('progress')
        collector = _ProgressCollector(progress)
        REGISTRY.register(collector)

    def add_rpc_metrics(self, client: RpcClient):
        self._add('rpc')
        collector = _RpcCollector(client)
        REGISTRY.register(collector)

    def serve(self, port: int):
        return start_wsgi_server(port)

    def _add(self, metric: str):
        if metric in self._metrics:
            raise Exception(f'{metric} metric was already registered')
        else:
            self._metrics.add(metric)


class _ProgressCollector(Collector):
    def __init__(self, progress: Progress):
        self._progress = progress

    def collect(self) -> Iterable[Metric]:
        progress = GaugeMetricFamily(
            'sqd_progress_blocks_per_second',
            'Overall block processing speed',
            self._progress.speed()
        )
        last_block = CounterMetricFamily(
            'sqd_last_block',
            'Last saved block',
            self._progress.get_current_value()
        )
        return [progress, last_block]


class _RpcCollector(Collector):
    def __init__(self, client: RpcClient):
        self._client = client

    def collect(self) -> Iterable[Metric]:
        metrics = self._client.metrics()
        rpc_served = CounterMetricFamily(
            'sqd_chain_rpc_requests_served',
            'Total number of served requests by connection',
            labels=['url'],
        )
        rpc_errors = CounterMetricFamily(
            'sqd_chain_rpc_connection_errors',
            'Total number of connection errors',
            labels=['url'],
        )
        rpc_avg_response_time = GaugeMetricFamily(
            'sqd_chain_avg_response_time_seconds',
            'Avg response time of connection',
            labels=['url'],
        )
        for metric in metrics:
            rpc_served.add_metric([metric.url], metric.served)
            rpc_errors.add_metric([metric.url], metric.errors)
            rpc_avg_response_time.add_metric([metric.url], metric.avg_response_time)
        return [rpc_served, rpc_errors, rpc_avg_response_time]
