import datetime
import os.path


def add_temp_prefix(path: str) -> str:
    now = datetime.datetime.now()
    ts = round(now.timestamp() * 1000)
    name = os.path.basename(path)
    dirname = os.path.dirname(path)
    return os.path.join(dirname, f'temp-{ts}-{name}')



