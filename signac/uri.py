# Copyright (c) 2018 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
import os

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


def link_to(resource, start=None):
    """Create a URL for a resource, such as a project or a job.

    This function will generate a URL for the provided resource
    that can then be retrieved *via* the :func:`.lookup` function.
    """
    return resource._as_link(start=start)


def lookup(url, start=None):
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
    return _lookup(cls=Project, url=url, start=start)


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


def _process_url_resource_request(cls, path, start):
    "Attempt to separate the resource and request specified in the provided path."
    nodes = path.split('/')
    n = len(nodes)
    if path:
        for i in range(n):
            try:
                root = os.path.join(* nodes[:n-i])
                project = _get_project_from_url_path(cls=cls, root=root, start=start)
            except LookupError:
                pass
            else:
                break
        else:
            raise LookupError(path)
        rest = nodes[n-i:]
        if rest:
            return project, os.path.normpath('/'.join(rest))
        else:
            return project, None
    else:
        return _get_project_from_url_path(cls=cls, root='.', start=start), None


def _lookup(cls, url, start=None):
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
    o = urlparse(url)
    path = os.path.expanduser(o.netloc + o.path)

    # Acquire project for given url.
    # Determine project for url.
    project, request = _process_url_resource_request(cls, path, start)

    if request:
        return _process_request_version_1(project, request, url)
    else:   # return resource directly
        # Open the job from state point for obtained project.
        if o.fragment:
            return project.open_job(id=o.fragment)
        else:
            return project


def _parse_url_filter_query(query):
    "Parse a filter specified as part of a url."
    kwargs = dict(map(lambda x: x.split('='), query.rstrip('&').split('&')))
    if len(kwargs) == 1 and 'filter' in kwargs:
        try:
            return unquote(json.loads(kwargs['filter']))
        except json.decoder.JSONDecodeError:
            pass
    return {k: _cast(v) for k, v in kwargs.items()}


def _parse_slicing_operator(slice_string):
    "Parse a slicing operator specified as string as part of a url."
    return slice(*map(lambda x: int(x.strip()) if x.strip() else None, slice_string.split(':')))


def _process_request_version_1(project, request, url):
    "Process a request as part of a url for API version 1."
    try:
        nodes = request.split('/')
        magic_word, version = nodes[:2]
        resource = nodes[2:]
        # With Py3: magic_word, version, *resource = request.split('/')
    except ValueError:
        raise InvalidRequestError(request)
    if magic_word == 'api':
        if version != 'v1':
            raise RuntimeError("Protocol version {} not supported.".format(version))
    else:
        raise InvalidRequestError(request)

    o = urlparse(url)
    if resource[0] == 'root':
        return project.root_director()
    elif resource[0] == 'workspace':
        return project.workspace()
    elif resource[0] == 'jobs':
        if o.query:
            filter = _parse_url_filter_query(o.query)
        else:
            filter = None
        jobs = project.find_jobs(filter)
        if o.fragment:
            if ':' in o.fragment:
                idx = _parse_slicing_operator(o.fragment)
                return list(jobs)[idx]
            else:
                return list(jobs)[int(o.fragment)]
        else:
            return jobs
    elif resource[0] == 'job':
        job = project.open_job(id=resource[1])
        if len(resource) > 3:
            if resource[2] == 'fn':
                return job.fn(os.path.join(* resource[3:]))
        elif len(resource) == 3:
            if resource[2] == 'fn':
                return job.workspace()
        elif len(resource) == 2:
            return job

    # The request must be invalid if we were not able to process the request up until this point.
    raise InvalidRequestError(request)
