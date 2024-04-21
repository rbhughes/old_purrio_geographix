import re
import math
from datetime import datetime


def xformer(xform_args):
    func_name, row, col, data_type, arg = xform_args

    def ensure_type(dtype, val):
        if val is None:
            return None
        elif dtype == "object":
            print("UNEXPECTED OBJECT TYPE! (needs xformer)")
            print(val)
            return None
        elif dtype == "string":
            return re.sub(r"[\u0000-\u001F\u007F-\u009F]", "", str(val))
        elif dtype == "number":
            if str(val).replace(" ", "") == "":
                return None
            try:
                n = float(val)
                return n if not math.isnan(n) else None
            except ValueError:
                return None
        elif dtype == "date":
            try:
                return datetime.fromisoformat(str(val)).isoformat()
            except (ValueError, TypeError):
                return None
        else:
            print(f"ENSURE TYPE SOMETHING ELSE (xformer): {type}")
            return "XFORM ME"

    if row.get(col) is None:
        return None

    ##################################################

    if func_name == "blob_to_hex":
        try:
            return row[col].hex()
        except (AttributeError, TypeError):
            print("ERROR")
            return None
    else:

        if data_type not in ("object", "string", "number", "date"):
            print("--------NEED TO ADD XFORM-------->", data_type)

        return ensure_type(data_type, row[col])


def doc_post_processor():
    print("here is doc post processor")


"""
    case "blob_to_hex":
     return (() => {
       try {
         return Buffer.from(obj[key]).toString("hex");
       } catch (error) {
         console.log("ERROR", error);
         return;
       }
     })();

"""
