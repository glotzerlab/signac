# Copyright (c) 2016 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
import hashlib
import json


def calc_id(spec):
    blob = json.dumps(spec, sort_keys=True)
    m = hashlib.md5()
    m.update(blob.encode())
    return m.hexdigest()
