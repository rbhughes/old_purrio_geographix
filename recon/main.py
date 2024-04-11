from pprint import pp
from recon.filesystem import dir_exists, normalize_path, glob_repos


#
#
def repo_recon(body):
    fs_path = normalize_path(body["recon_root"])

    is_dir = dir_exists(fs_path)

    repos = glob_repos(fs_path)

    # is_ggx = is_ggx_project(fs_path)

    print("-----------------")
    print("fs_path (norm)", fs_path)
    print("is_dir", is_dir)
    # pp(repos)

    print("-----------------")
    # return {"one": 111, "two": "asdfasdf"}
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
