import concurrent.futures
import logging
import os
import queue
import simplejson as json
import sys
import time

from dotenv import load_dotenv
from realtime.connection import Socket
from supabase import create_client

from asset.batcher import batcher
from asset.loader import loader
from common.logger import setup_logging, basic_log
from common.typeish import validate_task, validate_repo, Repo
from recon.recon import repo_recon
from search.search import search_local_pg

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
    return Socket(socket_url, auto_reconnect=True)


class PurrWorker:
    """
    PurrWorker client
    """

    def __init__(self):
        sb_url: str = os.environ.get("SUPABASE_URL")
        sb_key: str = os.environ.get("SUPABASE_KEY")
        self.supabase = create_client(sb_url, sb_key)
        self.work_queue = queue.Queue()
        self.search_queue = queue.Queue()
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
        Work Queue
        This gets invoked after PurrWorker instantiation in a separate thread:
        threading.Thread(target=pw.process_work_queue, daemon=True).start()
        :return: None
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

    def add_to_search_queue(self, task) -> None:
        """
        :param task: Adds a validated task to search queue.
        """
        self.search_queue.put(task)

    def process_search_queue(self) -> None:
        """
        Search Queue
        This gets invoked after PurrWorker instantiation in a separate thread:
        threading.Thread(target=pw.process_work_queue, daemon=True).start()
        :return: None
        """
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=int(os.environ.get("SEARCH_MAX_WORKERS"))
        ) as executor:
            while self.running or not self.search_queue.empty():
                try:
                    task = self.search_queue.get(block=False)
                    executor.submit(self.task_handler, task)
                except queue.Empty:
                    time.sleep(0.1)  # Avoid busy waiting

    def stop(self) -> None:
        """
        Halt the listening ThreadPoolExecutor(s)
        """
        self.running = False
        # sleep and then sys.exit()

    ########################################################################

    # def manage_task(self, task_id: int, status=None):
    def manage_task(self, task_id: int, status: Optional[str] = None) -> None:
        """
        We use the supabase task table with realtime as a queue. This method
        updates the task status and (later) deletes it
        :param task_id: An autoincrement int from supabase
        :param status: PENDING, PROCESSING or FAILED. see is_valid_status()
        :return: None
        """
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
        """
        A batcher task can spawn multiple loader (sub)tasks. We keep track of
        them in the supabase batch_ledger table.
        :param task_id: The normal task_id of a loader task
        :param batch_id: Think of it as the "parent" task_id (autoincr int)
        :param status: PENDING, PROCESSING or FAILED
        :return: None
        See also: manage_task() and is_batch_finished()
        """
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
        """
        As loader tasks are processed, check the batch_ledger table to see if
        any tasks remain for the given batch_id. All gone returns True
        :param batch_id: Think of it as the "parent" task_id (autoincr int)
        :return: bool: True if no loader tasks remain in batch_ledger. Note that
        any failed tasks (i.e. status = FAILED) will cause False.
        """
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
        """
        Get supabase user credentials from dotenv and sign_in
        :return: None
        """
        sb_email: str = os.environ.get("SUPABASE_EMAIL")
        sb_password: str = os.environ.get("SUPABASE_PASSWORD")
        self.supabase.auth.sign_in_with_password(
            {"email": sb_email, "password": sb_password}
        )

    def sign_out(self) -> None:
        """
        Sign out of Supbase and shut down
        :return: None
        """
        self.supabase.auth.sign_out()
        sys.exit()

    def fetch_repo(self, body) -> Repo:
        """
        Fetch a Repo from supabase and validate it as a "real" Repo dataclass
        :param body: The batcher and loader task body contains repo_id
        :return: an instance of Repo
        """
        res = self.supabase.table("repo").select("*").eq("id", body.repo_id).execute()
        repo = validate_repo((res.data[0]))
        return repo

    ###########################################################################

    def handle_batcher(self, task):
        """
        This task initiates asset collection by first counting the number of
        records, then creating (enqueueing) several loader tasks based on the
        chunk size. Fewer, larger chunks are faster, but risk memory problems.
        See comments in code.
        :param task: An instance of BatcherTask
        :return: TODO
        """
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

    ###########################################################################

    def handle_loader(self, task):
        """
        This task basically runs a select from the target project's gxdb and
        writes to the local postgresql database.
        :param task: An instance of LoaderTask
        :return: TODO
        """
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

    ###########################################################################

    def handle_recon(self, task):
        """
        This task recursively crawls the given filesystem path to locate
        GeoGraphix projects (i.e a repo). A well-centric metadata inventory is
        collected along with some directory stats. The repos discovered during
        recon are available as targets from which to collect assets.
        :param task: An instance of ReconTask
        :return: TODO
        """
        # 1. run repo_recon (returned as dicts for supabase)
        repos: List[Dict[str, Any]] = repo_recon(task.body)

        # 2. write repos to repo table
        self.supabase.table("repo").upsert(repos).execute()

        # 3. remove this task
        self.manage_task(task.id)

        for repo in repos:
            logger.info("RECON identified repo: " + repo["fs_path"])

    ###########################################################################
    def handle_search(self, task):

        res = search_local_pg(self.supabase, task.body)
        # make_asset_fts_queries(task.body)
        print("HANDLE_SEARCH!!!!!!!!!!!!!!!!!!!!!")
        print(res)

    ###########################################################################

    @basic_log
    def task_handler(self, task):

        self.manage_task(task.id, "PROCESSING")

        match task.directive:
            case "batcher":
                print("############################## batcher")
                self.handle_batcher(task)

            case "loader":
                print("############################## loader")
                self.handle_loader(task)

            case "recon":
                print("############################## recon")
                self.handle_recon(task)

            case "search":
                print("############################## search")
                self.handle_search(task)

            case "stats":
                print("############################## stats")

            case "halt":
                print("############################## halt")

        # self.sign_out()

    def listen(self):
        self.socket.connect()
        channel = self.socket.set_channel("realtime:public:task")

        def pluck(payload):
            task = validate_task(payload)
            if task:
                if task.directive == "search":
                    self.add_to_search_queue(task)
                else:
                    self.add_to_work_queue(task)

        channel.join().on("INSERT", pluck)
        channel.join().on("UPDATE", pluck)

        self.socket.listen()

    # @staticmethod
    # def say(x="nobody"):
    #     print("Hello World! ", x)
