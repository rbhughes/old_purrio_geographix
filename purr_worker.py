import os
import sys
from common.util import hostname
from realtime.connection import Socket
from supabase import create_client, Client
from dotenv import load_dotenv
from pprint import pp

from recon.main import repo_recon

load_dotenv()


def init_socket():
    sb_key: str = os.environ.get("SUPABASE_KEY")
    sb_id: str = os.environ.get("SUPABASE_ID")
    socket_url = (
        f"wss://{sb_id}.supabase.co/realtime/v1/websocket?apikey={sb_key}&vsn=1.0.0"
    )
    # socket = Socket(socket_url)
    socket = Socket(socket_url, auto_reconnect=True)
    return socket


#         "body": {
#             "ggx_host": "scarab",
#             "recon_root": "\\\\scarab\\ggx_projects\\sample",
#             "suite": "geographix",
#             "worker": "scarab",
#         },
#         "directive": "recon",
#         "id": 92899,
#         "status": "PENDING",
#         "worker": "scarab",


def valid_task(payload):
    try:
        if payload["record"]:
            worker = payload["record"]["worker"]
            if worker != hostname():
                print(f"Task for different worker: ({worker} != {hostname()})")
                return False
            suite = payload["record"]["body"]["suite"]
            status = payload["record"]["status"]
            if suite != "geographix" and status != "PENDING":
                print(f"Task suite != geographix ({suite}) or not 'PENDING'")
                return False
            return True
    except KeyError:
        # print("Not a task message")
        return False


class PurrWorker:
    """PurrWorker client class"""

    def __init__(self):
        sb_url: str = os.environ.get("SUPABASE_URL")
        sb_key: str = os.environ.get("SUPABASE_KEY")
        self.supabase: Client = create_client(sb_url, sb_key)
        self.socket = init_socket()
        self.sign_in()

    def sign_in(self):
        sb_email: str = os.environ.get("SUPABASE_EMAIL")
        sb_password: str = os.environ.get("SUPABASE_PASSWORD")
        self.supabase.auth.sign_in_with_password(
            {"email": sb_email, "password": sb_password}
        )

    def sign_out(self):
        self.supabase.auth.sign_out()
        sys.exit()

    def task_handler(self, payload):
        if not valid_task(payload):
            return

        directive: str = payload["record"]["directive"]
        body: dict = payload["record"]["body"]
        match directive:
            case "batcher":
                print("BATCHER")
            case "recon":
                print("RECON")
                res = repo_recon(body)
                pp(res)
                # returns list of repos
                # upserts repos
                # send message
                self.sign_out()

            case "search":
                print("SEARCH")
            case _:
                print("ANYTHING")

        # self.sign_out()

    def listen(self):
        self.socket.connect()
        channel = self.socket.set_channel("realtime:public:task")
        # channel.join().on("INSERT", callback1)
        channel.join().on("*", self.task_handler)
        self.socket.listen()

    # @staticmethod
    # def say(x="nobody"):
    #     print("Hello World! ", x)
