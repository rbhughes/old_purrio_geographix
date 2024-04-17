import os
import glob
from subprocess import run
from common.util import hostname, hashify
import re
from datetime import datetime
from common.util import normalize_path
from common.sqlanywhere import make_conn_params

DUPATH = "bin/du64.exe"


def is_ggx_project(maybe: str):
    gxdb = os.path.join(maybe, "gxdb.db")
    gxdb_prod = os.path.join(maybe, "gxdb_production.db")
    global_aoi = os.path.join(maybe, "Global")
    return (
        os.path.isfile(gxdb) and os.path.isfile(gxdb_prod) and os.path.isdir(global_aoi)
    )


def glob_repos(recon_root: str, ggx_host=f"{hostname().upper()}"):
    repo_list = []
    hits = glob.glob(os.path.join(recon_root, "**/gxdb.db"), recursive=True)
    for hit in hits:
        maybe = os.path.dirname(hit)
        if is_ggx_project(maybe):
            print(f".....GLOB.....{maybe}")
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
    return repo_list


def dir_stats(repo):
    """https://learn.microsoft.com/en-us/sysinternals/downloads/du"""
    fs_path = repo.get("fs_path")
    res = run(
        [DUPATH, "-q", "-nobanner", fs_path],
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


def repo_mod(repo: dict):
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

    traverse(repo["fs_path"])
    return {"repo_mod": last_mod.strftime("%Y-%m-%d %H:%M:%S")}
