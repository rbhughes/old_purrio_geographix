import pyodbc
import os
import re
from retry import retry
from common.util import normalize_path, RetryException
from common.typeish import SQLAnywhereConn

from typing import List, Dict, Any

# TODO: make this context aware, use "with..."


# @basic_log
@retry(RetryException, tries=5)
def db_exec(
    conn: dict | SQLAnywhereConn, sql: List[str] or str
) -> List[Dict[str, Any]] | List[List[Dict[str, Any]]]:
    """
    Connect to SQLAnywhere and Run SQL commands from str or list.
    Results are returned as {desc: (column description), rows: (list of rows)}.
    If sql is a single string, a single result dict is returned.
    If sql is a list of commands, results will be a list of dicts.

    If the gxdb.db file (conn['dbf']) is in use, exclude 'dbf' and retry to
    connect to an already-running database. This only works if params['dbn']
    exactly matches the name used by whatever process has the gxdb opened.

    Check active connections in dbisql via:
        select db_name( number ) from sa_db_list();

    :param conn: A SQLAnywhereConn object or equivalent dict
    :param sql: A single or list of SQL statements to run
    :return: dict or list of dicts, depending on the provided sql
    """

    if type(conn) is SQLAnywhereConn:
        conn = conn.to_dict()

    cursor = None
    try:
        connection = pyodbc.connect(**conn)
        cursor = connection.cursor()

        if isinstance(sql, str):
            results = []
            cursor.execute(sql)
            columns = [col[0] for col in cursor.description]
            for row in cursor.fetchall():
                res = dict(zip(columns, row))
                results.append(res)
            # return {'rowcount': int(cursor.rowcount), 'data': results}
            return results

        if isinstance(sql, list):
            multi = []
            for s in sql:
                results = []
                cursor.execute(s)
                columns = [col[0] for col in cursor.description]
                for row in cursor.fetchall():
                    res = dict(zip(columns, row))
                    results.append(res)
                multi.append(results)

                # multi.append({
                #     'rowcount': int(cursor.rowcount),
                #     'data': results
                # })

            return multi
    except pyodbc.OperationalError as oe:
        if re.search(r"Database name not unique", str(oe)):
            print(oe)
            conn.pop("dbf")
            raise RetryException from oe
    except Exception as ex:
        print(ex)
        raise ex

    finally:
        if cursor:
            cursor.close()
        # if connection:
        #     connection.close()


def make_conn_params(repo_path: str, host: str) -> dict:
    """
    :param repo_path: File path to the GeoGraphix project containing gxdb.db
    :param host: Ideally, this is the GeoGraphix project server's hostname
    :return: a dict containing SQLAnywhere connection parameters
    """
    ggx_host = host or "localhost"
    fs_path = normalize_path(repo_path)
    name = fs_path.split("/")[-1]
    home = fs_path.split("/")[-2]

    params = {
        "driver": os.environ.get("SQLANY_DRIVER"),
        "uid": "dba",
        "pwd": "sql",
        "host": ggx_host,
        "dbf": normalize_path(os.path.join(fs_path, "gxdb.db")),
        "dbn": name.replace(" ", "_") + "-" + home.replace(" ", "_"),
        "server": "GGX_" + ggx_host.upper(),
        "astart": "YES",
    }
    return params
