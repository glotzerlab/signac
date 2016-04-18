# run.py
import signac
import operations
from project import next_operation

project = signac.get_project()
for job in project.find_jobs():
    next_op = next_operation(job)
    if next_op is not None:
        func = getattr(operations, next_op)
        func(job)
