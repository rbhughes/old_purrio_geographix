import socket


def hostname():
    return socket.gethostname().lower()
