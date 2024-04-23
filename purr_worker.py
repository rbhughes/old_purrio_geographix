import os
import sys
import simplejson as json
import concurrent.futures
import queue
import time

import logging
from common.logger import setup_logging, basic_log

from realtime.connection import Socket
from supabase import create_client

# from common.util import hostname
from common.typeish import validate_task, validate_repo, Repo
from recon.recon import repo_recon
from asset.batcher import batcher
from asset.loader import loader
from dotenv import load_dotenv

from typing import Optional, Dict, List, Any

load_dotenv()
logger = logging.getLogger("purrio")
setup_logging("purr_worker")


def init_socket() -> Socket:
    """
    Initialize supabase realtime socket from .env. Project details are from:
    supabase.com/dashboard/<project>/settings/api
    :return: A realtime.connection Socket
    """
    sb_key: str = os.environ.get("SUPABASE_KEY")
    sb_id: str = os.environ.get("SUPABASE_ID")
    socket_url = (
        f"wss://{sb_id}.supabase.co/realtime/v1/websocket?apikey={sb_key}&vsn=1.0.0"
    )
    socket = Socket(socket_url, auto_reconnect=True)
    return socket


class PurrWorker:
    """
    PurrWorker client
    """

    def __init__(self):
        sb_url: str = os.environ.get("SUPABASE_URL")
        sb_key: str = os.environ.get("SUPABASE_KEY")
        self.supabase = create_client(sb_url, sb_key)
        self.work_queue = queue.Queue()
        # self.search_queue = queue.Queue()
        self.socket = init_socket()
        self.sign_in()
        self.running = True

    def add_to_work_queue(self, task) -> None:
        """
        :param task: Adds a validated task to work queue.
        """
        self.work_queue.put(task)

    def process_work_queue(self) -> None:
        """
        This gets invoked after PurrWorker instantiation in a separate thread:
        threading.Thread(target=pw.process_work_queue, daemon=True).start()
        """
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=int(os.environ.get("LOADER_MAX_WORKERS"))
        ) as executor:
            while self.running or not self.work_queue.empty():
                try:
                    task = self.work_queue.get(block=False)
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

    def stop(self) -> None:
        """
        Halt the listening ThreadPoolExecutor(s)
        """
        self.running = False

    ########################################################################

    # def manage_task(self, task_id: int, status=None):
    def manage_task(self, task_id: int, status: Optional[str] = None) -> None:
        if status is None:
            self.supabase.table("task").delete().eq("id", task_id).execute()
        elif status in ("PROCESSING", "FAILED"):
            (
                self.supabase.table("task")
                .update({"status": status})
                .eq("id", task_id)
                .execute()
            )

    def manage_asset_batch(self, task_id, batch_id, status=None) -> None:
        if status is None:
            (
                self.supabase.table("batch_ledger")
                .delete()
                .eq("batch_id", batch_id)
                .eq("task_id", task_id)
                .execute()
            )
        else:
            (
                self.supabase.table("batch_ledger")
                .update({"status": status})
                .eq("batch_id", batch_id)
                .eq("task_id", task_id)
                .execute()
            )

    def is_batch_finished(self, batch_id) -> bool:
        # pycharm inspector is wrong
        # https://anand2312.github.io/pgrest/reference/builders/
        # noinspection PyTypeChecker
        res = (
            self.supabase.table("batch_ledger")
            .select("*", count="exact")
            .eq("batch_id", batch_id)
            .execute()
        )
        return res.count == 0

    def sign_in(self) -> None:
        sb_email: str = os.environ.get("SUPABASE_EMAIL")
        sb_password: str = os.environ.get("SUPABASE_PASSWORD")
        self.supabase.auth.sign_in_with_password(
            {"email": sb_email, "password": sb_password}
        )

    def sign_out(self) -> None:
        self.supabase.auth.sign_out()
        sys.exit()

    def fetch_repo(self, body) -> Repo:
        res = self.supabase.table("repo").select("*").eq("id", body.repo_id).execute()
        repo = validate_repo((res.data[0]))
        # repo = Repo(**valid)
        return repo

    @basic_log
    def task_handler(self, task):

        self.manage_task(task.id, "PROCESSING")

        match task.directive:
            case "batcher":
                print("############################## batcher")

                # 1. get associated repo
                repo: Repo = self.fetch_repo(task.body)

                # 2. get asset dna from edge function
                res = self.supabase.functions.invoke(
                    task.body.suite,
                    invoke_options={"body": {"asset": task.body.asset}},
                )
                dna = json.loads(res.decode("utf-8"))

                # 3. define a batch of tasks
                tasks = batcher(task.body, dna, repo)

                # 4. enqueue batch of tasks (and get ids from return)
                upres = self.supabase.table("task").upsert(tasks).execute()

                # 5. update batch ledger too
                ledgers = [
                    {
                        "batch_id": batch_task.get("body").get("batch_id"),
                        "task_id": batch_task.get("id"),
                        "num_tasks": len(tasks),
                        "status": "PENDING",
                        "directive": "loader",
                    }
                    for batch_task in upres.data
                ]
                self.supabase.table("batch_ledger").upsert(ledgers).execute()

                # 6. remove this task
                self.manage_task(task.id)

            case "loader":
                logger.info("loader stuff")
                print("############################## loader")

                # 1. get associated repo
                repo = self.fetch_repo(task.body)

                # 2. run this loader task (select from source, write to pg)
                loader(task.body, repo)

                # 3. remove task from task table
                self.manage_task(task.id)

                # 4. remove batch/task combo from batch_ledger
                self.manage_asset_batch(task.id, task.body.batch_id)

                # 5. check if the whole batch is done
                done = self.is_batch_finished(task.body.batch_id)
                if done:
                    print("loader is really done DONE")

            case "recon":
                print("############################## recon")

                # 1. run repo_recon
                repos: List[Dict[str, Any]] = repo_recon(task.body)

                # 2. write repos to repo table
                self.supabase.table("repo").upsert(repos).execute()

                # 3. remove this task
                self.manage_task(task.id)

                for repo in repos:
                    logger.info(f"RECON found {repo["fs_path"]}")

            case "search":
                print("############################## search")

            case "stats":
                print("############################## search")

            case "halt":
                print("############################## halt")

        # self.sign_out()

    def listen(self):
        self.socket.connect()
        channel = self.socket.set_channel("realtime:public:task")

        def pluck(payload):
            task = validate_task(payload)
            if task:
                self.add_to_work_queue(task)

        channel.join().on("INSERT", pluck)
        channel.join().on("UPDATE", pluck)

        self.socket.listen()

    # @staticmethod
    # def say(x="nobody"):
    #     print("Hello World! ", x)
