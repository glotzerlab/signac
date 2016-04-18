# index.py
import signac

project = signac.get_project()

for doc in project.index():
    print(doc)

try:
    project.create_access_module()
except FileExistsError:
    pass
