
def add_temp_prefix(path: str) -> str:
    import datetime
    import os.path
    now = datetime.datetime.now()
    ts = round(now.timestamp() * 1000)
    name = os.path.basename(path)
    parent = os.path.dirname(path)
    return os.path.join(parent, f'temp-{ts}-{name}')


def sigterm_future():
    import asyncio
    import signal
    future = asyncio.get_event_loop().create_future()
    signal.signal(signal.SIGTERM, future.set_result)
    return future

