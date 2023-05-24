import logging

log = logging.getLogger(__name__)


def sync_loop(data_dir: str, updates_queue, new_chunks_queue):
    from etha.util.child_proc import init_child_process
    init_child_process()

    from etha.worker.state.folder import StateFolder

    while True:
        upd = updates_queue.get()
        try:
            StateFolder(data_dir).apply_update(
                upd,
                on_downloaded_chunk=lambda ds, chunk: new_chunks_queue.put((ds, chunk))
            )
        except Exception:
            log.exception('Downloading data chunks failed')
