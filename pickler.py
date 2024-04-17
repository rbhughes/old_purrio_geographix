from datetime import datetime
import math
import pickle
import simplejson as json
import base64

#
# def xformer(args):
#     func = args.get("func")
#     key = args.get("key")
#     typ = args.get("typ")
#     arg = args.get("arg")
#     obj = args.get("obj")
#
#     def ensure_type(type_str, val):
#         if val is None:
#             return None
#         elif type_str == "object":
#             print("UNEXPECTED OBJECT TYPE! (needs xformer)", type_str)
#             print(val)
#             return None
#         elif type_str == "string":
#             return val.replace(r"[\u0000-\u001F\u007F-\u009F]", "")
#         elif type_str == "number":
#             if str(val).replace(r"\s", "") == "":
#                 return None
#             n = float(val)
#             return n if not math.isnan(n) else None
#         elif type_str == "date":
#             try:
#                 return datetime.fromisoformat(val).isoformat()
#             except Exception:
#                 return None
#         else:
#             print("ENSURE TYPE SOMETHING ELSE (xformer)", type_str)
#             return "XFORM ME"
#
#     if obj.get(key) is None:
#         return None
#
#     if func == "blob_to_hex":
#         try:
#             return obj[key].hex()
#         except Exception as e:
#             print("ERROR", e)
#             return None
#     else:
#         return ensure_type(typ, obj[key])


# def my_function(x, y):
#     return x + y
#
#
# def pickle_to_json(func):
#     pickled_func = pickle.dumps(func)
#     base64_func = base64.b64encode(pickled_func).decode("utf-8")
#     json_func = json.dumps(base64_func)
#     return json_func
#
#
# def unpickle_from_json(json_func):
#     base64_func = json.loads(json_func)
#     pickled_func = base64.b64decode(base64_func.encode("utf-8"))
#     func = pickle.loads(pickled_func)
#     return func
#
#
# jjj = pickle_to_json(my_function)
# bbb = unpickle_from_json(jjj)
# x = bbb(3, 4)
# print(x)

# from time import sleep, perf_counter
# from concurrent.futures import ThreadPoolExecutor
#
#
# def task(id):
#     print(f"Starting the task {id}...")
#     sleep(1)
#     return f"Done with task {id}"
#
#
# def run_tasks(executor, tasks):
#     futures = []
#     for task_id in tasks:
#         futures.append(executor.submit(task, task_id))
#     return [f.result() for f in futures]
#
#
# start = perf_counter()
#
# with ThreadPoolExecutor(max_workers=5) as executor:
#     task_ids = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13]
#     results = run_tasks(executor, task_ids)
#     for result in results:
#         print(result)
#
# finish = perf_counter()
#
# print(f"It took {finish-start} second(s) to finish.")


def batch_selector2(args):
    asset_count, chunk, select, order, where = args

    batch = []
    start = 1
    x = 0

    while (asset_count - x) * chunk >= 0:
        if x + chunk > asset_count:
            chunk = asset_count - x + start
        else:
            chunk = x + chunk

        sql = (
            f"SELECT TOP {chunk} START AT {x} "
            f"{select.replace('SELECT', '', 1)} {where} {order};"
        )
        batch.append(sql)
        x += chunk

    return batch


def batch_selector(args):
    asset_count, chunk, select, order, where = args

    batch = []
    start = 1
    x = start

    while (asset_count - x) * chunk >= 0:
        # chunk = min(x + chunk, asset_count) - x + start
        chunk = asset_count - x + start if x + chunk > asset_count else chunk
        # min(chunk, asset_count - x)

        sql = (
            f"SELECT TOP {chunk} START AT {x} "
            f"{select.replace('SELECT', '', 1)} {where} {order};"
        )
        batch.append(sql)
        x += chunk

    return batch


batches = batch_selector((21, 40, "select statment", "order by", "where clause"))
for b in batches:
    print(b)
