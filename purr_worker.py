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


from recon.main import repo_recon
from asset.main import batcher

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
        return False


class PurrWorker:
    """PurrWorker client class"""

    def __init__(self):
        sb_url: str = os.environ.get("SUPABASE_URL")
        sb_key: str = os.environ.get("SUPABASE_KEY")
        self.supabase = create_client(sb_url, sb_key)
        self.loader_queue = queue.Queue()
        self.socket = init_socket()
        self.sign_in()
        self.running = True

    ##########################################

    def add_to_loader_queue(self, task):
        self.loader_queue.put(task)

    def process_queue(self):
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

    def stop(self):
        self.running = False

    # def taskZ(self, id):
    #     print(f"Starting the task {id}...")
    #     sleep(20)
    #     return f"Done with task {id}"
    #
    # def run_tasks(self, executor, tasks):
    #     futures = []
    #     for task_id in tasks:
    #         futures.append(executor.submit(self.taskZ, task_id))
    #     return [f.result() for f in futures]

    # def add_payload(self, payload):
    #     self.payloads.put(payload)

    # def process_tasks(self):
    #     futures = [
    #         self.executor.submit(task_func, *args, **kwargs)
    #         for task_func, args, kwargs in self.tasks
    #     ]
    #     for future in concurrent.futures.as_completed(futures):
    #         f = future.result()
    #         print(f)
    #     self.tasks.clear()
    # def process_tasks(self):
    #     """
    #     Processes all tasks in the queue using the ProcessPoolExecutor.
    #     """
    #     results = []
    #     while True:
    #         try:
    #             payload = self.payloads.get_nowait()
    #         except queue.Empty:
    #             break
    #         else:
    #             with self.executor:
    #                 future = self.executor.submit(self.task_handler, payload)
    #                 results.append(future.result())
    #     print(results)

    # def process_all_tasks(self):
    #     results = []
    #     with self.executor:
    #         futures = [
    #             self.executor.submit(self.process_task, task) for task in self.tasks
    #         ]
    #         for future in concurrent.futures.as_completed(futures):
    #             results.append(future.result())
    #     self.tasks.clear()
    #     return results

    ##########################################

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
            case "loader":
                print("################ loader")
                # print(body)
                return "fakeness loader"
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
                    print(body)
                    return

                repo = res.data[0]

                tasks = batcher(body, dna, repo)
                data, count = self.supabase.table("task").upsert(tasks).execute()

                # self.sign_out()

            case "recon":
                print("################ recon")
                res = repo_recon(body)
                data, count = self.supabase.table("repo").upsert(res).execute()
                print("------------------------------------------------")
                print(data)
                print(count)
                print("------------------------------------------------")

                # for r in res:
                #     pp(r)
                #     print("------------------------------------------------")
                # __ returns list of repos
                # __ upserts repos
                # __ send message
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
            self.add_to_loader_queue(payload)
            # print("pppppppp")
            # print(payload)
            # print("pppppppp")
            # future_task = self.executor.submit(self.task_handler, payload)
            # future_task.result()

            # with ThreadPoolExecutor(max_workers=5) as executor:
            #     task_ids = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13]
            #     results = self.run_tasks(executor, task_ids)
            #     for result in results:
            #         print(result)

            # with ThreadPoolExecutor(max_workers=5) as executor:
            #     # futureTask = executor.submit(self.taskZ, 3)
            #     future_task = executor.submit(self.task_handler, payload)
            #     print(future_task.result())

            # self.add_payload(payload)
            # self.process_tasks()

        # channel.join().on("*", self.task_handler)
        channel.join().on("*", pluck)

        # self.add_loader_task(self.task_handler, body)

        self.socket.listen()

    # @staticmethod
    # def say(x="nobody"):
    #     print("Hello World! ", x)
