
def sync_proc(data_dir: str, updates_queue, new_chunks_queue):
    from sqa.util.child_proc import init_child_process
    init_child_process()

    import logging
    import time
    from sqa.worker.state.folder import StateFolder

    log = logging.getLogger(__name__)

    while True:
        upd = updates_queue.get()
        retry_schedule = [1, 5, 10, 30, 60, 120, 300]
        retries = 0
        while True:
            try:
                log.info('update task started', extra={'update': upd})
                StateFolder(data_dir).apply_update(
                    upd,
                    on_downloaded_chunk=lambda ds, chunk: new_chunks_queue.put((ds, chunk))
                )
                log.info('update task finished')
                break
            except Exception:
                log.exception('update task failed')
                timeout = retry_schedule[min(retries, len(retry_schedule) - 1)]
                log.info(f'retrying update task in {timeout} seconds')
                time.sleep(timeout)
                retries += 1
