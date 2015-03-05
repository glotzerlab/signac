def generate_hash_from_spec(spec):
    import json, hashlib
    blob = json.dumps(spec, sort_keys = True)
    m = hashlib.md5()
    m.update(blob.encode())
    return m.hexdigest()

