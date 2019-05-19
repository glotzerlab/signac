# Copyright (c) 2019 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
import os
import re
from urllib.parse import urlparse, urlunparse

from .contrib.project import Project


_PATH_SCHEMA = r'(?P<root>.*?)(\/api\/v(?P<api_version>\d+)(?P<path>.*))'


def _open_v1(o, project, path):
    url = urlunparse(('signac', None, path.lstrip('/'), o.params, o.query, o.fragment))
    return project.open(url)


def open(url):
    """Open a signac URI."""
    o = urlparse(url)
    if o.netloc and o.netloc != 'localhost':
        raise NotImplementedError("Unable to open from remote host!")

    m = re.match(_PATH_SCHEMA, o.path)
    if m:
        g = m.groupdict()
        project = Project.get_project(os.path.abspath(g.pop('root')), search=False)
        api_version = g.pop('api_version')
        if api_version == '1':
            return _open_v1(o, project, **g)
        else:
            raise ValueError("Unknown API version '{}'.".format(api_version))
    elif o.path:
        return Project.get_project(os.path.abspath(o.path), search=False)
    else:
        raise ValueError("Invalid url '{}'.".format(url))
