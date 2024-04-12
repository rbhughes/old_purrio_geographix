import socket
import hashlib
import time
from functools import wraps
from datetime import datetime
import os


def timer(func):
    @wraps(func)
    def timer_wrapper(*args, **kwargs):
        t0 = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"[{func.__name__} START: {t0}]")
        start_time = time.perf_counter()
        result = func(*args, **kwargs)
        end_time = time.perf_counter()
        total_time = end_time - start_time
        t1 = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        elapsed = f"{total_time:.2f} seconds, {(total_time/60):.2f} minutes"
        print(f"[{func.__name__} END: {t1}] ~ {elapsed}")
        return result

    return timer_wrapper


def hostname():
    return socket.gethostname().lower()


def hashify(s: str):
    return hashlib.md5(s.lower().encode()).hexdigest()


def merge_nested_dict(a: dict, b: dict, path=None):
    """merges b into a"""
    if path is None:
        path = []
    for key in b:
        if key in a:
            if isinstance(a[key], dict) and isinstance(b[key], dict):
                merge_nested_dict(a[key], b[key], path + [str(key)])
            elif a[key] == b[key]:
                pass  # same leaf value
            else:
                print(f"dict merge conflict at {'.'.join(path + [str(key)])}")
        else:
            a[key] = b[key]
    return a


def dir_exists(fs_path: str):
    return os.path.isdir(fs_path)


def normalize_path(fs_path: str):
    return fs_path.replace("\\", "/")
