from common.sqlanywhere import db_exec
import simplejson as json
import psycopg2


def compose_docs(data, body):
    # xformer (native python for now
    # body.xforms
    # o.id from
    #     body.repo_id,
    #     body.asset,
    #     body.suite,
    #     body.asset_id_keys (mapped, joined)
    # body.well_id_keys (mapped, joined)
    # body.repo_id
    # body.repo_name
    # body.tag
    # body.suite
    # body.prefixes
    # serlalized_doc_processor
    print("___________________________in compose docs")
    # print(body.keys())
    # print("_________________________")
    print(body.get("xforms"))

    for row in data:
        o = {}
        doc = {}

        for x_key, x_val in body.get("xforms").items():
            print(x_key, "===", x_val.get("ts_type"), "----->", row[x_key])
        print("____________________________________________")

        for prefix, table in body.get("prefixes").items():
            print("prefix----------", prefix)
            print("table-----------", table)
            doc[table] = {}
            for key, val in row.items():
                if key.startswith(prefix):
                    new_key = key.replace(f"{prefix}", "")
                    doc[table][new_key] = val
        # o["doc"] = doc
        print(json.dumps(doc))


def loader(body, repo):

    try:
        data = db_exec(repo.get("conn"), body.get("selector"))
        # print(res)
        x = compose_docs(data, body)

    except Exception as e:
        print(e)
