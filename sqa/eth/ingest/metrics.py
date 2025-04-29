from typing import Iterable

from prometheus_client import start_wsgi_server
from prometheus_client.metrics_core import Metric, GaugeMetricFamily, CounterMetricFamily
from prometheus_client.registry import REGISTRY, Collector

from sqa.util.counters import Progress
from sqa.util.rpc import RpcClient
from sqa.eth.ingest.writer import ChunkWriter


class Metrics:
    def __init__(self):
        self._metrics = set()
        self._latest_received_block_number = 0
        self._latest_received_block_timestamp = 0
        self._latest_processed_block_timestamp = 0
        self._blocks_processing_time = 0

    def add_progress(self, progress: Progress, writer: ChunkWriter):
        self._add('progress')
        collector = _ProgressCollector(progress, writer, self)
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
            
    def set_latest_block_metrics(self, block_number: int, block_timestamp: int):
        self._latest_received_block_number = block_number
        self._latest_received_block_timestamp = block_timestamp
        
    def set_processing_metrics(self, block_timestamp: int, processing_time: int):
        self._latest_processed_block_timestamp = block_timestamp
        self._blocks_processing_time = processing_time
        
    def get_latest_received_block_number(self):
        return self._latest_received_block_number
        
    def get_latest_received_block_timestamp(self):
        return self._latest_received_block_timestamp
        
    def get_latest_processed_block_timestamp(self):
        return self._latest_processed_block_timestamp
        
    def get_blocks_processing_time(self):
        return self._blocks_processing_time


class _ProgressCollector(Collector):
    def __init__(self, progress: Progress, writer: ChunkWriter, metrics: Metrics):
        self._progress = progress
        self._writer = writer
        self._metrics = metrics

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
            self._writer.next_block - 1
        )
        
        last_written_block = GaugeMetricFamily(
            'sqd_dump_last_written_block',
            'Last written block',
            self._writer.next_block - 1
        )
        
        latest_received_block = GaugeMetricFamily(
            'sqd_latest_received_block_number',
            'Latest block number received',
            self._metrics.get_latest_received_block_number()
        )
        
        latest_received_timestamp = GaugeMetricFamily(
            'sqd_latest_received_block_timestamp',
            'Timestamp of latest received block',
            self._metrics.get_latest_received_block_timestamp()
        )
        
        latest_processed_timestamp = GaugeMetricFamily(
            'sqd_latest_processed_block_timestamp',
            'Timestamp of the latest processed block',
            self._metrics.get_latest_processed_block_timestamp()
        )
        
        blocks_processing_time = GaugeMetricFamily(
            'sqd_blocks_processing_time',
            'Time taken to process blocks (in seconds)',
            self._metrics.get_blocks_processing_time()
        )
        
        return [
            progress, 
            last_block, 
            last_saved_block, 
            last_written_block,
            latest_received_block,
            latest_received_timestamp,
            latest_processed_timestamp,
            blocks_processing_time
        ]


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
