import hashlib

import bson.json_util as json

def generate_hash_from_spec(spec):
    blob = json.dumps(spec, sort_keys = True)
    m = hashlib.md5()
    m.update(blob.encode())
    return m.hexdigest()

