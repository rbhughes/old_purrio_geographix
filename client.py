# from purr_worker import PurrWorker
# from common.util import hostname
# from src.recon.filesystem import glob_repos


###
import threading

from purr_worker import PurrWorker

print(__name__)


recon_payload = {
    "record": {
        "body": {
            "ggx_host": "scarab",
            "recon_root": "\\\\scarab\\ggx_projects\\north_park_basin",
            # "recon_root": "\\\\scarab\\ggx_projects",
            "suite": "geographix",
            "worker": "scarab",
        },
        "directive": "recon",
        "id": 92899,
        "status": "PENDING",
        "worker": "scarab",
    }
}

# batcher_payload = {
#     "record": {
#         "body": {
#             "asset": "well",
#             "chunk": 10,
#             "cron": "",
#             "where_clause": "w_uwi is not null",
#             "id": 76,
#             # "recency": 14,
#             "recency": 0,
#             "repo_fs_path": "//scarab/ggx_projects/Stratton",
#             "repo_id": "e68fa3e5-9e8b-18e0-e690-9839d0dc0f22",
#             "repo_name": "Stratton",
#             "suite": "geographix",
#             "tag": "GRINKLE",
#         },
#         "directive": "batcher",
#         "id": 666,
#         "status": "PENDING",
#         "worker": "scarab",
#     }
# }

batcher_payload = {
    "body": {
        "asset": "well",
        "chunk": 5,
        "cron": "",
        "id": 76,
        "recency": 0,
        "repo_fs_path": "//scarab/ggx_projects/stratton",
        "repo_id": "e68fa3e5-9e8b-18e0-e690-9839d0dc0f22",
        "repo_name": "stratton",
        "suite": "geographix",
        "tag": "GRINKLE",
        "where_clause": "",
    },
    "directive": "batcher",
    "id": 94598,
    "status": "PENDING",
    "worker": "scarab",
}

loader_payload = {
    "record": {
        "body": {
            "asset": "well",
            "chunk": 10,
            "cron": "",
            "where_clause": "w_uwi is not null",
            "id": 76,
            # "recency": 14,
            "recency": 0,
            "repo_fs_path": "//scarab/ggx_projects/Stratton",
            "repo_id": "e68fa3e5-9e8b-18e0-e690-9839d0dc0f22",
            "repo_name": "Stratton",
            "suite": "geographix",
            "tag": "GRINKLE",
        },
        "directive": "batcher",
        "id": 666,
        "status": "PENDING",
        "worker": "scarab",
    }
}


# ## ---

# ## ---


if __name__ == "__main__":
    pw = PurrWorker()
    threading.Thread(target=pw.process_loader_queue, daemon=True).start()
    pw.listen()
    # pw.task_handler(batcher_payload)
