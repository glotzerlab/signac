#
# Exporting the index
#

# config
db = signac.get_database(‘shape_potentials’)
index = db.index

gridfs_mirror = signac.fs.GridFS(db)
fs_mirror = signac.fs.LocalFileSystem('/data')

mirrors = (gridfs_mirror, fs_mirror)
defs = {'.*/\.gsd': gf.formats.GSDHOOMDTrajectoryFile}

# EXPORT
# Manual export
for doc in project.index(defs):
    index.replace_one({'_id': doc['_id']}, doc, upsert=True)
    for mirror in mirrors:
        with open(doc['filename'], 'rb') as file:
            mirror.put(_id=doc['_id'], file)

# Using a robust export function
export(project.index(defs), index, mirrors=mirrors)

# Exporting in context
with signac.use_mirrors(mirrors):
    export(project.index(defs), index)


# FETCHING
# Fetching a file from one mirror manually
from contextlib import closing
for doc in index.find():
    with closing(fs_mirror.get(doc['_id'])) as file:
        # Do something with file
        pass


# Using a robust fetch function
for doc in index.find():
    with signac.fetch(doc, mirrors=mirrors) as file:
        # Do something with file
        pass


#
# --- ROBUST IMPLEMENTATIONS (DETAIL) ---
#

def export(docs, index, mirrors=None, num_tries=3):
    "Robust export function."
    for doc in docs:
        for i in range(num_tries):
            try:
                index.replace_one({'_id': doc['_id']}, doc, upsert=True)
            except database.AutoRetry:
                logger.warning("Failed, retrying...")
            else:
                raise RuntimeError("Failed to export!")
        if mirrors:
            for mirror in mirrors:
                for i in range(num_tries):
                    try:
                        with open(doc['filename'], 'rb') as file:
                            mirror.put(_id=doc['md5'], doc)
                    except mirror.FileExistsError:
                        break
                    except mirror.AutoRetry:
                        logger.warning("Failed to mirror file, retrying...")
                    else:
                        break
                else:
                    raise RuntimeError("Failed to mirror file!")


def fetch(doc_or_id, mirrors=None, num_tries=3):
    "Robust fetch function."
    if mirrors is None:
        mirrors = MIRRORS
    doc = doc_or_id if isinstance(doc_or_id, str) else doc['_id']
    for i in range(num_tries):
        for mirror in mirros:
            try:
                return mirror.get(doc['_id'])
                break
            except mirror.FileNotFoundError as error:
                logger.warning(error)
        else:
            raise signac.errors.FileNotFoundError(doc['_id'])
