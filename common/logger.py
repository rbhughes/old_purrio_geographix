import functools
import logging
import os
import sys
from datetime import datetime
from logging import Logger

from typing import Callable

# CRITICAL
# ERROR
# WARNING
# INFO
# DEBUG

LOG_DIR = os.environ.get("LOG_DIR") or "logs"


def setup_logging(tag=None) -> Logger:
    """
    Initialize a named logger

    NOTE: enabling DEBUG can generate HUGE log files, as they include SQL
    statements and collected data. It is best to insert WHERE clause filters or
    otherwise limit the data before debugging!

    ANOTHER NOTE: It would be nice to have the logs rotate, but that turns
    out to be quite complicated since log writes come from multithreaded
    processes. The "official" way is to use a QueueHandler...yuck.

    :param tag: Optional tag name to insert into the logfile name
    :return: Logger
    """

    # ts = datetime.now().strftime("%Y-%m-%d_%H:%M:%S")
    ts = datetime.now().strftime("%Y-%m-%d_%H_%M_%S")

    logfile = os.path.join(LOG_DIR, f"{tag}_{ts}_purrio.log")
    print("Initialized log file: " + logfile)

    logger = logging.getLogger("purrio")
    logger.setLevel(logging.INFO)
    # logger.setLevel(logging.DEBUG)

    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

    if "console_handler" not in [x.name for x in logger.handlers]:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.DEBUG)
        console_handler.setFormatter(formatter)
        console_handler.set_name("console_handler")
        logger.addHandler(console_handler)

    if "file_handler" not in [x.name for x in logger.handlers]:
        file_handler = logging.FileHandler(logfile, mode="a")
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(formatter)
        file_handler.set_name("file_handler")
        logger.addHandler(file_handler)

    return logger


# https://ankitbko.github.io/blog/2021/04/logging-in-python/
def basic_log(func) -> Callable:
    """
    Wrapped decorator that logs just about everything. Just add @basic_log
    :param func: Any function
    :return: Callable wrapper
    """
    logger = logging.getLogger("purrio")

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        args_repr = [repr(a) for a in args]
        kwargs_repr = [f"{k}={v!r}" for k, v in kwargs.items()]
        signature = ", ".join(args_repr + kwargs_repr)
        logger.debug(f"[{func.__name__}] w/args: {signature}")
        try:
            result = func(*args, **kwargs)
            logger.debug(f"[{func.__name__}] result: {result}")
            return result
        except Exception as e:
            logger.exception(
                f"Exception raised in [{func.__name__}]. exception: {str(e)}"
            )
            raise e

    return wrapper
