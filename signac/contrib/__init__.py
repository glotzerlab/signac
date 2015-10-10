import os
import warnings

from ..common.config import load_config
from .project import Project

def get_project(project_path = None):
    if project_path is not None:
        cwd = os.getcwd()
        os.chdir(project_path)
        project = Project()
        os.chdir(cwd)
    else:
        project = Project()
    project.get_id()
    return project
