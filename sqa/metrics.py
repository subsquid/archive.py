import threading
from logging import getLogger
from multiprocessing import Queue

from prometheus_client import Counter, Enum, Gauge, Info, Summary, start_http_server

LOG = getLogger(__name__)

WORKER_INFO = Info('worker_info', 'Worker info')
WORKER_STATUS = Enum(
    'worker_status',
    'Status of the worker',
    states=['starting', 'not_registered', 'unsupported_version', 'jailed', 'active']
)
QUERY_OK = Counter('num_successful_queries', 'Number of queries which executed successfully')
BAD_REQUEST = Counter('num_bad_requests', 'Number of received invalid queries')
SERVER_ERROR = Counter('num_server_errors', 'Number of queries which resulted in server error')
RESULT_SIZE = Summary('query_result_size_bytes', '(Gzipped) result size of an executed query (bytes)')
READ_CHUNKS = Summary('num_read_chunks', 'Number of chunks read during query execution')
EXEC_TIME = Summary('query_exec_time_ms', 'Time spent processing query')
STORED_BYTES = Gauge('stored_bytes', 'Total bytes stored in the data directory')

DOWNLOADED_BYTES = Summary('downloaded_bytes', 'Total downloaded bytes')
DOWNLOAD_OK = Counter('num_successful_downloads', 'Number of successfully downloaded files')
DOWNLOAD_ERROR = Counter('num_download_errors', 'Number of failed file downloads')


class MetricsServer:
    """ This class allows sending download metrics from a subprocess.
        We're not using Prometheus multiprocessing mode, because it doesn't work well with gauge, info, and enum.
    """

    DOWNLOAD_METRICS_QUEUE = None

    @classmethod
    def on_download(cls, success: bool, downloaded_bytes: int = 0) -> None:
        if cls.DOWNLOAD_METRICS_QUEUE is not None:
            cls.DOWNLOAD_METRICS_QUEUE.put((success, downloaded_bytes))

    @classmethod
    def _start_download_metrics_thread(cls):
        def _process_metrics(queue):
            while True:
                success, downloaded_bytes = queue.get(block=True)
                if success:
                    DOWNLOAD_OK.inc()
                else:
                    DOWNLOAD_ERROR.inc()
                if downloaded_bytes > 0:
                    DOWNLOADED_BYTES.observe(downloaded_bytes)

        download_metrics_thread = threading.Thread(
            target=_process_metrics,
            args=(cls.DOWNLOAD_METRICS_QUEUE,),
            daemon=True
        )
        download_metrics_thread.start()

    @classmethod
    def start(cls, port):
        assert cls.DOWNLOAD_METRICS_QUEUE is None, "Metrics server already running"
        LOG.info(f"Exposing prometheus metrics on port {port}")
        start_http_server(port)
        cls.DOWNLOAD_METRICS_QUEUE = Queue(maxsize=100)
        cls._start_download_metrics_thread()
