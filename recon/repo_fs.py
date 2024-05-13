import glob
import os
import re
from datetime import datetime
from subprocess import run
from common.util import normalize_path, hostname, hashify
from common.sqlanywhere import make_conn_params


# from common.logger import setup_logging, auto_log
from common.logger import Logger

# import logging

from typing import List


DUPATH = "bin/du64.exe"


logger = Logger(__name__)

# logger = logging.getLogger("purrio")
# setup_logging("repo_fs")

###
# def print_unmatched_files(func):
#     def wrapper(*args, **kwargs):
#         gen = func(*args, **kwargs)
#         try:
#             while True:
#                 result = next(gen)
#                 if isinstance(result, str):
#                     print(f"Non-matching file: {result}")
#         except StopIteration:
#             pass
#         print("DDDDDDDDDDDDDDDDDDDD")
#         print(list(gen))
#         print("DDDDDDDDDDDDDDDDDDDD")
#         return list(gen)
#
#     return wrapper
###


def is_ggx_project(maybe: str) -> bool:
    """
    A basic file/folder structure test to determine if a directory looks like a
    geographix project. Assumes a healthy structure.
    :param maybe: Any directory path
    :return: True if input path looks like a repo directory
    """
    gxdb = os.path.join(maybe, "gxdb.db")
    gxdb_prod = os.path.join(maybe, "gxdb_production.db")
    global_aoi = os.path.join(maybe, "Global")
    return (
        os.path.isfile(gxdb) and os.path.isfile(gxdb_prod) and os.path.isdir(global_aoi)
    )


# @print_unmatched_files


# @basic_log
# @auto_log
def glob_repos(recon_root: str, ggx_host=f"{hostname().upper()}") -> List[dict]:
    repo_list = []
    hits = glob.glob(os.path.join(recon_root, "**/gxdb.db"), recursive=True)
    for hit in hits:
        maybe = os.path.dirname(hit)
        if is_ggx_project(maybe):
            # print(f".....GLOB.....{maybe}")
            logger.info(f".....GLOB.....{maybe}")
            repo_list.append(
                {
                    "id": hashify(normalize_path(maybe)),
                    "name": os.path.basename(maybe),
                    "fs_path": normalize_path(maybe),
                    "conn": make_conn_params(maybe, ggx_host),
                    "conn_aux": {"ggx_host": ggx_host},
                    "suite": "geographix",
                }
            )
        else:
            logger.debug(f"NOT A PROJECT: {maybe}")
            # logger.send_message(f"WHAT ARE YOU GONNA DO ABOUT {maybe}")
    #
    # print("RRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRR")
    # print(repo_list)
    # print("RRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRR")
    return repo_list


def dir_stats(repo_base) -> dict:
    """
    https://learn.microsoft.com/en-us/sysinternals/downloads/du
    Run microsoft's du utility to collect directory size. Faster than python.
    :param repo_base: A stub repo dict. We just use the fs_path
    :return: dict of parsed stdout byte sizes
    """
    res = run(
        [DUPATH, "-q", "-nobanner", repo_base["fs_path"]],
        capture_output=True,
        text=True,
        check=False,
    )
    meta = {}
    lines = res.stdout.splitlines()
    for line in lines:
        if line:
            pair = line.split(":")
            left = pair[0].strip()
            right = pair[1].replace("bytes", "").replace(",", "").strip()
            if left == "Size":
                meta["bytes"] = int(right)
            elif left != "Size on disk":
                meta[left.lower()] = int(right)
    return meta


def repo_mod(repo_base) -> dict:
    """
    Recursively traverse project folders to determine the most recently modified
    project file date. We exclude SQLAnywhere, since the act of connecting to it
    will update the mod dates. No, this is not performant.
    TODO: optimize this
    :param repo_base: A stub repo dict. We just use the fs_path
    :return: dict with repo_mode date
    """
    last_mod = datetime(1970, 1, 1)

    def traverse(dir_path: str):
        nonlocal last_mod
        for f in os.listdir(dir_path):
            full_path = os.path.join(dir_path, f)
            try:
                stat = os.stat(full_path)
                if os.path.isdir(full_path):
                    traverse(full_path)
                else:
                    gxdb_matcher = r"gxdb\.db$|gxdb_production\.db$|gxdb\.log$"
                    if not re.match(gxdb_matcher, f):
                        mod_time = datetime.fromtimestamp(stat.st_mtime)
                        if mod_time > last_mod:
                            last_mod = mod_time
            except (OSError, ValueError):
                continue

    traverse(repo_base["fs_path"])
    return {"repo_mod": last_mod.strftime("%Y-%m-%d %H:%M:%S")}
