from pprint import pp
from recon.repo_fs import glob_repos, dir_stats, repo_mod
from recon.repo_db import well_counts, hull_outline
from common.util import dir_exists, normalize_path
from recon.epsg import epsg_codes


def repo_recon(body):
    fs_path = normalize_path(body["recon_root"])

    # is_dir = dir_exists(fs_path)

    repos = glob_repos(fs_path)

    # is_ggx = is_ggx_project(fs_path)

    # print("----------------")
    # print("fs_path (norm)", fs_path)
    # print("is_dir", is_dir)
    # print("----------------")
    # return {"one": 111, "two": "asdfasdf"}

    for repo in repos:
        for func in [
            well_counts,
            epsg_codes,
            dir_stats,
            repo_mod,
            hull_outline,
        ]:
            md = func(repo)
            repo.update(md)

    return repos


#
#
# body = {
#     "ggx_host": "scarab",
#     "recon_root": "\\\\scarab\\ggx_projects\\sample",
#     "suite": "geographix",
#     "worker": "scarab",
# }
#
#
# repo_recon(body)

"""
import asyncio

async def process_repo(repo):
    for func in [well_counts, epsg_codes, dir_stats, repo_last_mod]:
        md = await func(repo)
        repo.update(md)

async def main():
    tasks = []
    for repo in repos:
        tasks.append(asyncio.create_task(process_repo(repo)))
    await asyncio.gather(*tasks)

if __name__ == "__main__":
    asyncio.run(main())
"""
