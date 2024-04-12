import socket
import hashlib


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
