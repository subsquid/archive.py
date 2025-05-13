from typing import Iterable

from prometheus_client import Metric
from prometheus_client.metrics_core import GaugeMetricFamily, CounterMetricFamily
from prometheus_client.registry import Collector

from . import Sink


class SinkMetricsCollector(Collector):
    def __init__(self, sink: Sink):
        self.sink = sink

    def collect(self) -> Iterable[Metric]:
        yield GaugeMetricFamily(
            'sqd_progress_blocks_per_second',
            'Overall block processing speed',
            self.sink.get_progress_speed()
        )
        yield CounterMetricFamily(
            'sqd_last_block',
            'Last ingested block',
            self.sink.get_last_seen_block()
        )
        yield CounterMetricFamily(
            'sqd_last_saved_block',
            'Last saved block',
            self.sink.get_last_flushed_block(),
        )
        yield GaugeMetricFamily(
            'sqd_latest_processed_block_number',
            'Latest processed block number',
            self.sink.get_last_flushed_block()
        )
        yield GaugeMetricFamily(
            'sqd_latest_processed_block_timestamp',
            'Timestamp of the latest processed block',
            self.sink.get_last_block_timestamp()
        )   
