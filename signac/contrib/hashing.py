import hashlib
import json


def calc_id(spec):
    blob = json.dumps(spec, sort_keys=True)
    m = hashlib.md5()
    m.update(blob.encode())
    return m.hexdigest()
