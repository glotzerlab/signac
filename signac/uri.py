# Copyright (c) 2018 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
import os
from collections import namedtuple

from .core.json import json
from .common import six
from .contrib.project import Project
from .contrib.filterparse import _cast

if six.PY2:
    from urlparse import urlparse
    from urlparse import unquote
else:
    from urllib.parse import urlparse
    from urllib.parse import unquote


class InvalidRequestError(ValueError):
    "Indicates that a request specified as part of a URL is invalid."
    pass


ParseResult = namedtuple(
    'ParseResult',
    ['scheme', 'root', 'path', 'query', 'fragment'])


def link_to(resource, start=None):
    """Create a URL for a resource, such as a project or a job.

    This function will generate a URL for the provided resource
    that can then be retrieved *via* the :func:`.retrieve` function.
    """
    return resource._as_url(start=start)


def retrieve(url, start=None):
    """Locate a resource, such as a project or a job from the provided URL.

    :seealso: :py:func:`.link_to`

    :param url:
        The URL that links to the referenced resource.
    :raises InvalidRequestError:
        If the request specified in the URL is invalid.
    :raises LookupError:
        If a project specified by a URL cannot be found.
    :raises KeyError:
        If a job specified by a URL does not exist.
    """
    return _retrieve(cls=Project, url=url, start=start)


def _get_project_from_url_path(cls, root, start):
    "Lookup the project for the given root and start paths."
    if start is None:
        if os.path.isabs(root):
            return cls.get_project(root=root)
        else:
            start = cls.get_project().root_directory()
            return cls.get_project(root=os.path.join(start, root))
    else:
        try:
            return cls.get_project(root=os.path.join(start.root_directory(), root))
        except AttributeError:
            return cls.get_project(root=os.path.join(start, root))


def _urlparse(url):
    o = urlparse(url)
    path = os.path.expanduser(o.netloc + o.path)
    if ':' in path:
        root_path, path = path.split(':', 1)
    else:
        root_path, path = path, None

    return ParseResult(
        scheme=o.scheme,
        root=root_path,
        path=path,
        query=o.query,
        fragment=o.fragment,
    )


def _retrieve(cls, url, start=None):
    """Lookup the resource specified in url.

    :param url:
        The URL that specifies the desired resource.
    :raises LookupError:
        If a project specified by a link cannot be found or the
        provided job id is ambiguous.
    :raises KeyError:
        If a job specified by a link cannot be found.
    """
    # Parse the provided url
    o = _urlparse(url)

    # Acquire project for given url.
    project = _get_project_from_url_path(cls, o.root, start)
    return _process_request_version_1(project, o)


def _parse_url_filter_query(query):
    "Parse a filter specified as part of a url."
    kwargs = dict(map(lambda x: x.split('='), query.rstrip('&').split('&')))
    if len(kwargs) == 1 and 'filter' in kwargs:
        return json.loads(unquote(kwargs['filter']))
    return {k: _cast(v) for k, v in kwargs.items()}


def _parse_slicing_operator(slice_string):
    "Parse a slicing operator specified as string as part of a url."
    return slice(*map(lambda x: int(x.strip()) if x.strip() else None, slice_string.split(':')))


def _process_request_document(document, path):
    if path:
        nodes = path.split('.')
        v = document[nodes[0]]
        for node in nodes[1:]:
            v = v[node]
        return v
    else:
        return document


def _process_request_job(job, path):
    if path and '/' in path:
        head, tail = path.split('/', 1)
    else:
        head, tail = path, ''

    if head == 'workspace':
        return os.path.join(job.workspace(), tail)
    elif head == 'document':
        return _process_request_document(job.document, tail)
    elif head:
        raise InvalidRequestError("Unknown request: '{}'.".format(head))
    else:
        return job


def _process_request_jobs(project, query):
    if query:
        filter = _parse_url_filter_query(query)
    else:
        filter = None
    return project.find_jobs(filter)


def _process_request_version_1(project, o):
    if o.path and '/' in o.path:
        head, tail = o.path.split('/', 1)
    else:
        head, tail = o.path, ''

    if head == 'job':
        if tail and '/' in tail:
            jobid, path = tail.split('/', 1)
        else:
            jobid, path = tail, ''
        job = project.open_job(id=jobid)
        return _process_request_job(job, path)
    elif head == 'jobs':
        return _process_request_jobs(project, o.query)
    elif head == 'workspace':
        return os.path.join(project.workspace(), tail)
    elif head == 'root':
        return os.path.join(project.root_directory())
    elif head == 'document':
        return _process_request_document(project.document, tail)
    elif head:
        raise InvalidRequestError("Unknown request '{}'.".format(head))
    else:
        return project


__all__ = ['InvalidRequestError', 'link_to', 'retrieve']
