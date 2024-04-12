# from purr_worker import PurrWorker
# from common.util import hostname
# from src.recon.filesystem import glob_repos

###

from purr_worker import PurrWorker

print(__name__)


payload = {
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

# ## ---

# ## ---


if __name__ == "__main__":
    pw = PurrWorker()
    pw.listen()
    # pw.task_handler(payload)
