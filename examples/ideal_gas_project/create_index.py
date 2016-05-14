# create_index.py
import signac
from signac.contrib.formats import TextFile

project = signac.get_project()

for doc in project.index({'.*/V\.txt': TextFile}):
    print(doc)

try:
    project.create_access_module({'.*/V\.txt': TextFile})
except OSError:
    print("Access module already exists!")
