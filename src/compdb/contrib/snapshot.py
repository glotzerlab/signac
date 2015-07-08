import logging
logger = logging.getLogger(__name__)

COLLECTIONS_EXCLUDE = ['system']

import pymongo
PYMONGO_3 = pymongo.version_tuple[0] == 3

def dump_db(db, dst):
    import os
    from bson import json_util as serializer
    try:
        os.makedirs(dst)
    except Exception:
        raise
    for collection in db.collection_names():
        skip = False
        for exclude in COLLECTIONS_EXCLUDE:
            if collection.startswith(exclude):
                skip = True
                break
        if skip:
            continue
        logger.debug("Dumping '{}'.".format(collection))
        fn = os.path.join(dst, collection) + '.json'
        with open(fn, 'wb') as file:
            for doc in db[collection].find():
                file.write("{}\n".format(
                    serializer.dumps(doc)).encode())

def restore_db(db, dst):
    from os.path import join
    from bson import json_util as serializer
    for collection in db.collection_names():
        skip = False
        for exclude in COLLECTIONS_EXCLUDE:
            if collection.startswith(exclude):
                skip = True
                break
        if skip:
            continue
        logger.debug("Trying to restore '{}'.".format(collection))
        fn = join(dst, collection) + '.json'
        try:
            with open(fn, 'rb') as file:
                db[collection].drop()
                for line in file:
                    doc = serializer.loads(line.decode())
                    if PYMONGO_3:
                        db[collection].insert_one(doc)
                    else:
                        db[collection].save(doc)
        except FileNotFoundError:
            pass
