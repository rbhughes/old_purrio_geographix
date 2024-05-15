import simplejson as json
from common.logger import Logger
from common.sqlanywhere import db_exec
from common.util import hashify, hostname
from typing import List

logger = Logger(__name__)


def batch_selector(args) -> List[str]:
    """
     Given total and chunk size, define a batch of SQL selects.
     The count is limited by SQLAnywhere's 'SELECT TOP X START AT Y' syntax.
     Example  (total: 1137, chunk: 500):
     sql: 'select * from well order by uwi'

    'SELECT TOP 500 START AT 1 * from well order by uwi',
    'SELECT TOP 500 START AT 501 * from well order by uwi',
    'SELECT TOP 137 START AT 1001 * from well order by uwi'

     :param args: a tuple containing the following parameters:
         chunk: number of rows to collect per batch
         select: a SQL SELECT statement
         order: a SQL ORDER BY clause
         where: a SQL WHERE clause
     :return: list of SQL select statements
    """
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


def batcher(body, dna, repo) -> List[dict]:
    """
    Used in conjunction with a Supabase Edge function, this constructs SQL
    select statements to collect asset data from the GXDB.
    :param body: An instance of BatcherTaskBody
    :param dna: A JSON object from the GeoGraphix edge function(s)
    :param repo: The target project
    :return: A list of loader task defintions to be enqueued
    """

    logger.send_message(
        directive="note",
        repo_id=repo.id,
        data={"note": f"define batcher tasks: {body.asset}: " + repo.fs_path},
        workflow="load",
    )

    # dna...
    select: str = dna.get("select")
    purr_recent: str = dna.get("purr_recent")
    order: str = dna.get("order")

    # inject recency WHERE clause into select if specified
    if body.recency > 0:
        recent_peg = (
            f"WHERE row_changed_date >= DATEADD(DAY, -{body.recency}, CURRENT " f"DATE)"
        )
    else:
        recent_peg = ""
    select = select.replace(purr_recent, recent_peg)

    # construct WHERE clause
    where_parts = ["WHERE 1=1"]
    if len(body.where_clause.strip()) > 0:
        where_parts.append(body.where_clause)
    where = " AND ".join(where_parts)

    # get asset count
    count = f"SELECT COUNT(*) AS count FROM ( {select} ) c {where}"
    res = db_exec(repo.conn, count)
    if not res or len(res) == 0:
        # print("batcher error: could not get asset count")
        print(f"No {body.suite} {body.asset} records found in .")
        return []
    asset_count: int = res[0].get("count")

    # get batches
    chunk = body.chunk
    selectors = batch_selector((asset_count, chunk, select, order, where))

    tasks = []
    for selector in selectors:
        task_body = {
            "asset": body.asset,
            "tag": body.tag,
            "asset_id_keys": dna.get("asset_id_keys"),
            "batch_id": hashify(json.dumps(body.to_dict()).lower()),
            "conn": repo.conn.to_dict(),
            "suite": repo.suite,
            "prefixes": dna.get("prefixes"),
            "purr_delimiter": dna.get("purr_delimiter"),
            "purr_null": dna.get("purr_null"),
            "repo_id": repo.id,
            "repo_name": repo.name,
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
