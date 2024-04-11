import os
import glob

from common.util import hostname

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
                    "name": os.path.basename(maybe),
                    "fs_path": normalize_path(maybe),
                    # "conn": make_conn_params(maybe, ggx_host),
                    "conn_aux": {"ggx_host": ggx_host},
                }
            )
    return repo_list
