import datetime
import sys


def log_time(message: str) -> None:
    ct = datetime.datetime.now()
    print(f"[{ct}] {message}", file=sys.stderr)
    return
