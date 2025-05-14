from typing import Iterable
import time

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
        
        current_time = int(time.time())
        block_timestamp = self.sink.get_last_block_timestamp()
        processing_time = 0 if block_timestamp == 0 else current_time - block_timestamp
        yield GaugeMetricFamily(
            'sqd_blocks_processing_time',
            'Time difference between now and block timestamp (in seconds)',
            processing_time
        )   
