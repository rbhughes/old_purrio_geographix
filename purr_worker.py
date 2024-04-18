import os
import sys
from common.util import hostname
from realtime.connection import Socket
from supabase import create_client
from dotenv import load_dotenv
import simplejson as json
import concurrent.futures
import queue

import time
from concurrent.futures import ThreadPoolExecutor


from recon.recon import repo_recon
from asset.batcher import batcher
from asset.loader import loader

load_dotenv()

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


def init_socket():
    sb_key: str = os.environ.get("SUPABASE_KEY")
    sb_id: str = os.environ.get("SUPABASE_ID")
    socket_url = (
        f"wss://{sb_id}.supabase.co/realtime/v1/websocket?apikey={sb_key}&vsn=1.0.0"
    )
    socket = Socket(socket_url, auto_reconnect=True)
    return socket


def validate_task(payload):
    try:
        if payload.get("record"):
            if (
                payload.get("record").get("worker") == hostname()
                and payload.get("record").get("status") == "PENDING"
                and payload.get("record").get("body").get("suite") == "geographix"
            ):
                return payload.get("record")
    except KeyError as ke:
        print(ke)
        return None


class PurrWorker:
    """PurrWorker client class"""

    def __init__(self):
        sb_url: str = os.environ.get("SUPABASE_URL")
        sb_key: str = os.environ.get("SUPABASE_KEY")
        self.supabase = create_client(sb_url, sb_key)
        self.loader_queue = queue.Queue()
        # self.search_queue = queue.Queue()
        self.socket = init_socket()
        self.sign_in()
        self.running = True

    ##########################################

    def add_to_loader_queue(self, task):
        self.loader_queue.put(task)

    # gets invoked in separate thread
    def process_loader_queue(self):
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=int(os.environ.get("LOADER_MAX_WORKERS"))
        ) as executor:
            while self.running or not self.loader_queue.empty():
                try:
                    task = self.loader_queue.get(block=False)
                    # executor.submit(self.run, task)
                    executor.submit(self.task_handler, task)
                except queue.Empty:
                    time.sleep(0.1)  # Avoid busy waiting

    # gets invoked in separate thread
    # def process_search_queue(self):
    #     with concurrent.futures.ThreadPoolExecutor(
    #         max_workers=int(os.environ.get("SEARCH_MAX_WORKERS"))
    #     ) as executor:
    #         while self.running or not self.search_queue.empty():
    #             try:
    #                 task = self.search_queue.get(block=False)
    #                 # executor.submit(self.run, task)
    #                 executor.submit(self.task_handler, task)
    #             except queue.Empty:
    #                 time.sleep(0.1)  # Avoid busy waiting

    def stop(self):
        self.running = False

    ########################################################################

    def manage_sb_task(self, task, status=None):
        if status is None:
            # print("____SHOULD DELETE TASK", task.get("id"), status)
            data, count = (
                self.supabase.table("task").delete().eq("id", task.get("id")).execute()
            )
            # print(data, count)
        elif status in ("PROCESSING", "FAILED"):
            # print("____SHOULD UPDATE TASK", task.get("id"), status)
            data, count = (
                self.supabase.table("task")
                .update({"status": status})
                .eq("id", task.get("id"))
                .execute()
            )
            # print(data, count)

    def sign_in(self):
        sb_email: str = os.environ.get("SUPABASE_EMAIL")
        sb_password: str = os.environ.get("SUPABASE_PASSWORD")
        self.supabase.auth.sign_in_with_password(
            {"email": sb_email, "password": sb_password}
        )

    def sign_out(self):
        self.supabase.auth.sign_out()
        sys.exit()

    def task_handler(self, task):

        self.manage_sb_task(task, "PROCESSING")

        directive: str = task.get("directive")
        body: dict = task.get("body")

        match directive:
            case "loader":
                print("################ loader")
                print("loader...")

                res = (
                    self.supabase.table("repo")
                    .select("*")
                    .eq("id", body.get("repo_id"))
                    .execute()
                )
                if not res.data:
                    print(f"batcher error: repo_id not found")

                repo = res.data[0]

                loader(body, repo)

                self.manage_sb_task(task)

                # return "fakeness loader"
                # self.add_loader_task(self.task_handler, body)
                # self.sign_out()

            case "batcher":
                print("################ batcher")

                res = self.supabase.functions.invoke(
                    body.get("suite"),
                    invoke_options={"body": {"asset": body.get("asset")}},
                )
                dna = json.loads(res.decode("utf-8"))

                res = (
                    self.supabase.table("repo")
                    .select("*")
                    .eq("id", body.get("repo_id"))
                    .execute()
                )
                if not res.data:
                    print(f"batcher error: repo_id not found")
                    # print(body)
                    # return

                repo = res.data[0]

                tasks = batcher(body, dna, repo)
                data, count = self.supabase.table("task").upsert(tasks).execute()
                print("---------batcher--------------------------------")
                # print(data, count)
                # print("------------------------------------------------")
                self.manage_sb_task(task)

                # self.sign_out()

            case "recon":
                print("################ recon")
                res = repo_recon(body)
                data, count = self.supabase.table("repo").upsert(res).execute()
                print("---------recon----------------------------------")
                # print(data, count)
                # print("------------------------------------------------")
                self.manage_sb_task(task)

                # self.sign_out()

            case "search":
                print("SEARCH")
            case _:
                print("ANYTHING")

        # self.sign_out()

    def listen(self):
        self.socket.connect()
        channel = self.socket.set_channel("realtime:public:task")

        def pluck(payload):
            task = validate_task(payload)
            self.add_to_loader_queue(task)

        channel.join().on("INSERT", pluck)
        channel.join().on("UPDATE", pluck)

        self.socket.listen()

    # @staticmethod
    # def say(x="nobody"):
    #     print("Hello World! ", x)
