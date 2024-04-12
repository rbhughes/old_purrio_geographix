import os
import glob
from subprocess import run
from common.util import hostname, hashify
import re
from datetime import datetime

DUPATH = "bin/du64.exe"
#
#
# print(hostname())


def dir_exists(fs_path: str):
    return os.path.isdir(fs_path)


def normalize_path(fs_path: str):
    return fs_path.replace("\\", "/")


def is_ggx_project(maybe: str):
    gxdb = os.path.join(maybe, "gxdb.db")
    gxdb_prod = os.path.join(maybe, "gxdb_production.db")
    global_aoi = os.path.join(maybe, "Global")
    return (
        os.path.isfile(gxdb) and os.path.isfile(gxdb_prod) and os.path.isdir(global_aoi)
    )


def glob_repos(recon_root: str, ggx_host=f"GGX_{hostname().upper()}"):
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


def make_conn_params(repo_path: str, host: str):
    """doc"""
    ggx_host = host or "localhost"
    fs_path = normalize_path(repo_path)
    name = fs_path.split("/")[-1]
    home = fs_path.split("/")[-2]

    params = {
        "driver": os.environ.get("SQLANY_DRIVER"),
        "uid": "dba",
        "pwd": "sql",
        "host": ggx_host,
        "dbf": normalize_path(os.path.join(fs_path, "gxdb.db")),
        "dbn": name.replace(" ", "_") + "-" + home.replace(" ", "_"),
        "server": "GGX_" + ggx_host.upper(),
        "astart": "YES",
    }
    return params


# def sizeof_fmt(num, suffix="B"):
#     """doc"""
#     num = int(num)
#     for unit in ["", "K", "M", "G", "T", "P", "E", "Z"]:
#         if abs(num) < 1024.0:
#             return f"{num:3.1f}{unit}{suffix}"
#         num /= 1024.0
#     return f"{num:.1f}Y{suffix}"


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
                # right = sizeof_fmt(right)
                # meta[left.lower()] = right
                # meta["human_size"] = right
            elif left != "Size on disk":
                meta[left.lower()] = int(right)
    # return {"inventory": meta}
    return meta


# async def repo_last_mod(repo_path: str):
#     last_mod = datetime(1970, 1, 1)
#
#     async def traverse(dir_path: str):
#         nonlocal last_mod
#         for f in os.listdir(dir_path):
#             full_path = os.path.join(dir_path, f)
#             stat = os.stat(full_path)
#             if os.path.isdir(full_path):
#                 await traverse(full_path)
#             else:
#                 if not re.match(r"gxdb\.db$|gxdb_production\.db$|gxdb\.log$", f):
#                     if stat.st_mtime >= last_mod.timestamp():
#                         last_mod = datetime.fromtimestamp(stat.st_mtime)
#
#     await traverse(repo_path)
#     return {"repo_mod": last_mod.strftime("%Y-%m-%d %H:%M:%S")}


def repo_last_mod(repo: dict):
    last_mod = datetime(1970, 1, 1)
    print("=======================", last_mod)

    def traverse(dir_path: str):
        nonlocal last_mod
        for f in os.listdir(dir_path):
            full_path = os.path.join(dir_path, f)
            try:
                stat = os.stat(full_path)
                if os.path.isdir(full_path):
                    traverse(full_path)
                else:
                    if not re.match(r"gxdb\.db$|gxdb_production\.db$|gxdb\.log$", f):
                        mod_time = datetime.fromtimestamp(stat.st_mtime)
                        if mod_time > last_mod:
                            last_mod = mod_time
            except (OSError, ValueError):
                continue

    traverse(repo["fs_path"])
    return {"repo_mod": last_mod.strftime("%Y-%m-%d %H:%M:%S")}
