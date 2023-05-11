

def init_child_process() -> None:
    from etha.log import init_logging
    init_logging()

    import signal
    signal.signal(signal.SIGINT, signal.SIG_IGN)
