from pprint import pp
from common.sqlanywhere import db_exec
from common.util import hashify, hostname
import simplejson as json

# body = {
#     "asset": "well",
#     "chunk": 100,
#     "cron": "",
#     "filter": "",
#     "id": 76,
#     "recency": 14,
#     "repo_fs_path": "//scarab/ggx_projects/Stratton",
#     "repo_id": "e68fa3e5-9e8b-18e0-e690-9839d0dc0f22",
#     "repo_name": "Stratton",
#     "suite": "geographix",
#     "tag": "GRINKLE",
# }


def batch_selector(args):
    asset_count, chunk, select, order, where = args

    batch = []
    start = 1
    x = start

    while (asset_count - x) * chunk >= 0:
        chunk = asset_count - x + start if x + chunk > asset_count else chunk
        sql = (
            f"SELECT TOP {chunk} START AT {x} "
            f"{select.replace('SELECT', '', 1)} {where} {order};"
        )
        batch.append(sql)
        x += chunk

    return batch


def batcher(body, dna, repo):
    # body...
    recency = body.get("recency")
    where_clause = body.get("where_clause")

    # dna...
    select: str = dna.get("select")
    where_recent_slot: str = dna.get("where_recent_slot")
    order: str = dna.get("order")

    # inject recency WHERE clause into select if specified
    if recency > 0:
        recent_peg = f"WHERE row_changed_date >= DATEADD(DAY, -{recency}, CURRENT DATE)"
    else:
        recent_peg = ""
    select = select.replace(where_recent_slot, recent_peg)

    # construct WHERE clause
    where_parts = ["WHERE 1=1"]
    if len(where_clause.strip()) > 0:
        where_parts.append(where_clause)
    where = " AND ".join(where_parts)

    # get asset count
    count = f"SELECT COUNT(*) AS count FROM ( {select} ) c {where}"
    res = db_exec(repo.get("conn"), count)
    if not res or len(res) == 0:
        print("batcher error: could not get asset count")
        return
    asset_count: int = res[0].get("count")

    # get batches
    chunk = body.get("chunk")
    selectors = batch_selector((asset_count, chunk, select, order, where))

    tasks = []
    for selector in selectors:
        task_body = {
            "asset": body.get("asset"),
            "tag": body.get("tag"),
            "asset_id_keys": dna.get("asset_id_keys"),
            "batch_id_keys": hashify(json.dumps(body).lower()),
            "conn": repo.get("conn"),
            "suite": repo.get("suite"),
            "prefixes": dna.get("prefixes"),
            "repo_id": repo.get("id"),
            "repo_name": repo.get("name"),
            "selector": selector,
            "well_id_keys": dna.get("well_id_keys"),
            "xforms": dna.get("xforms"),
        }
        tasks.append(
            {
                "worker": hostname(),
                "directive": "loader",
                "status": "PENDING",
                "body": task_body,
            }
        )

    return tasks
