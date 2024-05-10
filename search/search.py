import psycopg2
import re

from psycopg2 import sql
from common.typeish import SearchTaskBody
from common.util import local_pg_params

from typing import List, Dict


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

    for q in fts_queries:
        with conn.cursor() as cur:
            cur.execute(q["sql"])
            res = cur.fetchall()
        hits = (
            [
                {
                    "search_id": body.search_id,
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
        print("loading hits to supabase...", len(hits))
        supabase.table("search_result").upsert(hits).execute()

    return "maybe donezo"
