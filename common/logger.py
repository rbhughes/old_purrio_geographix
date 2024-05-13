import logging
import os
import sys

from dotenv import load_dotenv

from common.sb_client import SupabaseClient

from common.messenger import Messenger

# CRITICAL
# ERROR
# WARNING
# INFO
# DEBUG

load_dotenv()
LOG_DIR = os.environ.get("LOG_DIR") or "logs"
NAME = "purrio"


class Logger:
    """
    Move the regular logger instantiation to a singleton class
    Messenger is
    """

    _instance = None

    # ensure singleton class
    def __new__(cls, source):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance.__init__(source)
        return cls._instance

    def __init__(self, source):
        # ts = datetime.now().strftime("%Y-%m-%d_%H_%M_%S")
        # logfile = os.path.join(LOG_DIR, f"{ts}_purrio.log")
        logfile = os.path.join(LOG_DIR, f"{NAME}.log")
        # print("Initialized log file: " + logfile)

        self.logger = logging.getLogger(NAME)
        self.logger.setLevel(logging.DEBUG)
        # self.logger.setLevel(logging.INFO)

        self.sb_client = SupabaseClient()
        self.messenger = Messenger(self.sb_client)

        formatter = logging.Formatter(
            f"%(asctime)s - %(name)s - %(levelname)s - {source} | %(message)s"
        )

        if "console_handler" not in [x.name for x in self.logger.handlers]:
            console_handler = logging.StreamHandler(sys.stdout)
            console_handler.setLevel(logging.DEBUG)
            console_handler.setFormatter(formatter)
            console_handler.set_name("console_handler")
            self.logger.addHandler(console_handler)

        if "file_handler" not in [x.name for x in self.logger.handlers]:
            file_handler = logging.FileHandler(logfile, mode="a")
            file_handler.setLevel(logging.DEBUG)
            file_handler.setFormatter(formatter)
            file_handler.set_name("file_handler")
            self.logger.addHandler(file_handler)

    def critical(self, message):
        self.logger.critical(message)

    def error(self, message):
        self.logger.error(message)

    def warning(self, message):
        self.logger.warning(message)

    def info(self, message):
        self.logger.info(message)

    def debug(self, message):
        self.logger.debug(message)

    def send_message(self, directive, repo_id, data):
        self.messenger.send(directive, repo_id, data)
