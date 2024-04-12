from typing import List, Dict
import re
import pyodbc
from retry import retry

# from .logger import basic_log


class RetryException(Exception):
    """Just a trigger to catch gxdb.db in use exception"""


# @basic_log
@retry(RetryException, tries=2)
def db_exec(conn: Dict, sql: List[str] or str):
    """Connect to SQLAnywhere and Run SQL commands from str or list.
    Results are returned as {desc: (column description), rows: (list of rows)}.
    If sql is a single string, a single result dict is returned.
    If sql is a list of commands, results will be a list of dicts.
    If the gxdb.db file (conn['dbf']) is in use, exclude 'dbf' and retry to
    connect to an already-running database. This only works if params['dbn']
    exactly matches the name used by whatever process has the gxdb opened.
    check in dbisql: select db_name( number ) from sa_db_list();
    """
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
        if connection:
            connection.close()
