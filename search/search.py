import csv
import json
import psycopg2
import psycopg2.extras
import re

from psycopg2 import sql
from common.logger import Logger
from common.typeish import SearchTaskBody
from common.util import local_pg_params
from contextlib import closing
from typing import List, Dict

logger = Logger(__name__)


def make_asset_fts_queries(body: SearchTaskBody, conn: psycopg2.extensions.connection):
    fts_queries: List[Dict[str, str]] = []

    for asset in body.assets:
        query = sql.SQL(
            "SELECT "
            "repo_id, repo_name, well_id, suite, tag, doc, {field} as asset "
            "FROM {table} a "
            "WHERE "
            "1=1 AND"
        ).format(
            field=sql.Literal(asset),
            table=sql.Identifier(asset),
        )

        query += sql.SQL(" a.suite IN ({})").format(
            sql.SQL(",").join(map(sql.Literal, body.suites))
        )

        # screen out blanks; wildcard chars treated literally here
        is_valid_tag = (
            body.tag and isinstance(body.tag, str) and re.search(r"\S", body.tag)
        )
        if is_valid_tag:
            query += sql.SQL(" AND a.tag = {}").format(sql.Literal(body.tag))

        # screen out terms comprised of only wildcards/spaces
        is_valid_terms = (
            body.terms
            and isinstance(body.terms, str)
            and not re.match(r"^[*?\s]+$", body.terms)
        )
        if is_valid_terms:
            terms = [
                (
                    re.sub(r"\*", ":*", re.sub(r"\?", "_", term))
                    if re.search(r"[*?]", term)
                    else term
                )
                for term in re.split(r"\s+", body.terms)
                if term.strip()
            ]
            if terms:
                tsquery = " & ".join(terms)
                query += sql.SQL(" AND a.ts @@ to_tsquery('english', {})").format(
                    sql.Literal(tsquery)
                )

        fts_queries.append({"sql": query.as_string(conn), "asset": asset})

    # print("=========search q==================================")
    # for q in fts_queries:
    #     print(q)
    #     # s = q["sql"].as_string(conn)  # not kosher, just checking
    #     # print(s)
    # print("===============++++++++============================")
    return fts_queries


def search_local_pg(supabase, body: SearchTaskBody) -> str:
    conn: psycopg2.extensions.connection = psycopg2.connect(**local_pg_params())

    fts_queries: List[Dict[str, str]] = make_asset_fts_queries(body, conn)

    limit = 100
    summary = []

    for q in fts_queries:

        summary.append({"asset": q["asset"], "sql": q["sql"]})

        with conn.cursor() as cur:
            # query = q["sql"] if body.save_to_store else q["sql"] + " LIMIT 100"
            cur.execute(q["sql"] + f" LIMIT {limit}")
            res = cur.fetchall()

            cur.execute(f"SELECT COUNT(*) FROM ({q["sql"]}) AS subquery;")
            total_hits = cur.fetchone()[0]

            for d in summary:
                if d["asset"] == q["asset"]:
                    d["total_hits"] = total_hits

            # if total_hi
            # hits > 100:
            #     print("TTTTTTTTTTTTTTTTTT")
            #     print("more than 100 hits. want to save to file?")
            #     print("TTTTTTTTTTTTTTTTTT")

            # TODO: send to search_results instead of message
            # logger.send_message(
            #     directive="storage_prompt",
            #     data={"note": f"fts search yields: {total_hits} hits. Save?"},
            #     workflow="search",
            # )

        hits = (
            [
                {
                    "search_id": body.search_id,
                    "directive": "search_result",
                    "asset": q["asset"],
                    "active": True,
                    "search_body": body.to_dict(),
                    "sql": q["sql"],
                    "user_id": body.user_id,
                }
            ]
            if cur.rowcount == 0
            else [
                {
                    "search_id": body.search_id,
                    "directive": "search_result",
                    "asset": q["asset"],
                    "active": True,
                    "search_body": body.to_dict(),
                    "sql": q["sql"],
                    "user_id": body.user_id,
                    "repo_id": row[0],
                    "repo_name": row[1],
                    "well_id": row[2],
                    "suite": row[3],
                    "tag": row[4],
                    "doc": row[5],
                }
                for row in res
            ]
        )

        logger.send_message(
            directive="note",
            data={"note": f"fts for " f"{q["asset"]} yields: {len(hits)} hits"},
            workflow="search",
        )

        if int(total_hits) > 0:
            supabase.table("search_result").upsert(hits).execute()

    supabase.table("search_result").insert(
        {
            "search_id": body.search_id,
            "user_id": body.user_id,
            "directive": "storage_prompt",
            "search_body": summary,
        }
    ).execute()

    return "maybe donezo"


def query_to_file(query, out_file, conn_params, file_format="csv"):
    """
    Execute a SQL query and write the results to a CSV file.

    Args:
        query (str): The SQL query to execute.
        out_file (str): The path to the output CSV file.
        conn_params (dict): A dictionary containing the connection parameters
            for the PostgreSQL database.
        file_format: csv or json

    Returns:
        None
    """
    batch_size = 1000

    try:
        with closing(psycopg2.connect(**conn_params)) as conn:
            with closing(conn.cursor(cursor_factory=psycopg2.extras.DictCursor)) as cur:
                cur.execute(query)

                first_row = cur.fetchone()
                if not first_row:
                    print("No data returned from query.")
                    return

                # Because we select more than just doc
                colnames = [desc[0] for desc in cur.description]
                doc_index = colnames.index("doc")

                json_keys = list(first_row[doc_index].keys())
                headers = json_keys

                if file_format == "csv":
                    with open(out_file, mode="w", newline="") as csvfile:
                        csv_writer = csv.writer(csvfile)

                        # Write the headers to the CSV file
                        csv_writer.writerow(headers)

                        # Write the first row to the CSV file
                        json_data = first_row[doc_index]  # JSONB column data
                        csv_row = (
                            [json_data.get(key) for key in json_keys]
                            if json_data
                            else [None] * len(json_keys)
                        )
                        csv_writer.writerow(csv_row)

                        # Fetch and write in batches
                        while True:
                            rows = cur.fetchmany(size=batch_size)
                            if not rows:
                                break
                            for row in rows:
                                json_data = row[doc_index]
                                csv_row = (
                                    [json_data.get(key) for key in json_keys]
                                    if json_data
                                    else [None] * len(json_keys)
                                )
                                csv_writer.writerow(csv_row)

                elif file_format == "json":
                    data = []

                    # Add the first row to the data list
                    json_data = first_row[doc_index]  # JSONB column data
                    data.append(json_data if json_data else {})

                    # Fetch and add rows in batches
                    while True:
                        rows = cur.fetchmany(size=batch_size)
                        if not rows:
                            break
                        for row in rows:
                            json_data = row[doc_index]
                            data.append(json_data if json_data else {})

                    # Write the JSON data to the output file
                    with open(out_file, mode="w") as jsonfile:
                        json.dump(data, jsonfile, indent=4)

        print(f"Data successfully written to {out_file}")

    except Exception as e:
        import traceback

        print(f"An error occurred: {e}")
        traceback.print_exc()
