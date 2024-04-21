from common.sqlanywhere import db_exec
from common.util import hashify
import simplejson as json
import psycopg2
import psycopg2.extras
import os

from asset.xformer import xformer


def local_pg_params():
    return {
        "user": "postgres",
        "host": "localhost",
        "database": "postgres",
        "password": os.environ.get("LOCAL_PG_PASS"),
        "port": 5432,
    }


ASSET_COLUMNS = ["id", "repo_id", "repo_name", "well_id", "suite", "tag", "doc"]


def make_upsert_stmt(table_name, columns):
    cols = columns.copy()
    stmt = [f"INSERT INTO {table_name}"]
    stmt.append(f"({', '.join(columns)})")
    stmt.append("VALUES")
    placeholders = ", ".join(["%s"] * len(columns))
    stmt.append(f"({placeholders})")
    stmt.append("ON CONFLICT (id) DO UPDATE SET")
    cols.pop(0)
    stmt.append(", ".join([f"{col} = EXCLUDED.{col}" for col in columns]))
    return " ".join(stmt)


def pg_upserter(docs, table_name):
    conn = None
    cursor = None
    try:
        conn = psycopg2.connect(**local_pg_params())
        cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        psycopg2.extensions.register_adapter(dict, psycopg2.extras.Json)

        upsert_stmt = make_upsert_stmt(table_name, ASSET_COLUMNS)

        upsert_count = 0
        cursor.execute("BEGIN")

        for doc in docs:
            ordered_data = [doc.get(col) for col in ASSET_COLUMNS]
            cursor.execute(upsert_stmt, ordered_data)
            upsert_count += cursor.rowcount

        conn.commit()

        print(f"Successful upsert: {upsert_count} of {len(docs)} {table_name}")

    except (Exception, psycopg2.Error) as error:
        print(error)
        conn.rollback()
        print(error)

    finally:
        if conn:
            cursor.close()
            conn.close()
        # return "potato salad"


def compose_docs(data, body):
    docs = []

    for row in data:
        o = {}
        doc = {}

        o["id"] = hashify(
            str(body.get("repo_id"))
            + str(body.get("asset"))
            + str(body.get("suite"))
            + str(body.get("repo_id"))
            + "".join([str(row[k]) for k in body.get("asset_id_keys")])
        )

        o["well_id"] = "-".join([str(row[k]) for k in body.get("well_id_keys")])
        o["repo_id"] = body.get("repo_id")
        o["repo_name"] = body.get("repo_name")
        o["tag"] = body.get("tag")
        o["suite"] = body.get("suite")

        # invoke xformer
        for col, xf in body.get("xforms").items():
            data_type = xf.get("ts_type")
            func_name = xf.get("xform")
            xform_args = (func_name, row, col, data_type, None)
            row[col] = xformer(xform_args)

        # build json based on prefixes
        for prefix, table in body.get("prefixes").items():
            doc[table] = {}
            for key, val in row.items():
                if key.startswith(prefix):
                    new_key = key.replace(f"{prefix}", "", 1)
                    doc[table][new_key] = val
        o["doc"] = doc
        docs.append(o)

    return docs


def loader(body, repo):

    try:
        data = db_exec(repo.get("conn"), body.get("selector"))
        # print(res)
        docs = compose_docs(data, body)
        # print(json.dumps(docs, indent=2))
        res = pg_upserter(docs, body.get("asset"))

    except Exception as e:
        print(e)
