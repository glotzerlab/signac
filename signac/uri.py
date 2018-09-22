# Copyright (c) 2018 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
import os
import logging
from collections import namedtuple
from functools import partial

from .core.json import json
from .core.utility import parse_version
from .common import six
from .contrib.project import Project
from .contrib.filterparse import _cast

if six.PY2:
    from urlparse import urlparse
    from urlparse import unquote
else:
    from urllib.parse import urlparse
    from urllib.parse import unquote


logger = logging.getLogger(__name__)


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
    return resource._to_url(start=start)


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

    # We do not distinguish between the net-location and path.
    path = os.path.expanduser(o.netloc + o.path)

    # Split url into the project root directory and the remaining path.
    if ':' in path:
        root_path, path = path.split(':', 1)
    else:
        root_path, path = path, None

    # Parse the query string.
    if o.query:
        query = dict(map(lambda x: x.split('='), o.query.rstrip('&').split('&')))
    else:
        query = dict()

    # Return parsed result as named tuple for further processing.
    return ParseResult(
        scheme=o.scheme,      # The url scheme, assumed to be 'signac'
        root=root_path,       # The path to the project root directory.
        path=path,            # An optional path for further resource specification.
        query=query,          # An optional query dictionary.
        fragment=o.fragment,  # The fragment bit.
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

    api_version = parse_version(o.query.get('api', '1.0'))
    if api_version.to_tuple()[0] == 1:
        return _process_resource_project_api_version_1(project, o)
    else:
        raise InvalidRequestError(
            "Unable to process request for API version '{}'.".format(api_version))


def _parse_url_filter_query(query):
    "Parse a filter specified as part of a url."
    if len(query) == 1 and 'filter' in query:
        return json.loads(unquote(query['filter']))
    else:
        return {k: _cast(v) for k, v in query.items()}


def _parse_slicing_operator(slice_string):
    "Parse a slicing operator specified as string as part of a url."
    return slice(*map(lambda x: int(x.strip()) if x.strip() else None, slice_string.split(':')))


def _split(path):
    if path and '/' in path:
        return path.split('/', 1)
    else:
        return path, ''


def _process_resource_document(document, path, query):
    if path:
        nodes = path.replace('/', '.').split('.')
        v = document[nodes[0]]
        for node in nodes[1:]:
            v = v[node]
        return v
    else:
        return document


def _process_resource_schema(project, path, query):
    head, tail = _split(path)
    if head:
        return project.detect_schema(** query)[head]
    else:
        return project.detect_schema(** query)


def _process_resource_index(project, path, query):
    index = project.index(** query)
    get = partial(_process_resource_document, path=path, query=query)
    return map(get, index)


def _process_resource_job(job, path, query):
    head, tail = _split(path)
    if head == 'id':
        return job.get_id()
    elif head in ('ws', 'workspace'):
        return os.path.join(job.workspace(), tail)
    elif head in ('sp', 'statepoint'):
        return _process_resource_document(job.statepoint, tail, query)
    elif head in ('doc', 'document'):
        return _process_resource_document(job.document, tail, query)
    elif head:
        raise InvalidRequestError("Unknown request: '{}'.".format(head))
    else:
        return job


def _process_resource_jobs(project, query):
    if query:
        filter = _parse_url_filter_query(query)
    else:
        filter = None
    return project.find_jobs(filter)


def _open_job_with_redirects(project, job_id, history=None):
    "Open job with possible redirect."
    try:
        return project.open_job(id=job_id)
    except KeyError as error:
        if history is None:  # Initialize history if necessary.
            history = set()

        # Raise error if this particular was already encountered.
        if job_id in history:
            raise RuntimeError("Detected inifinite loop!")
        else:
            history.add(job_id)

        # Check whether this particular id is in the redirect mapping and return job.
        try:
            redirects = project.doc._redirects
            redirected_id = redirects[job_id]
            logger.warning("Job '{}' was redirected to '{}'.".format(job_id, redirected_id))
            return _open_job_with_redirects(project, redirected_id, history)
        except AttributeError:
            # no redirect mapping
            raise error
        except KeyError:
            candidates = {_id for _id in redirects if _id.startswith(job_id)}
            if len(candidates) == 1:
                return _open_job_with_redirects(project, candidates.pop(), history)
            elif len(candidates) > 1:
                raise LookupError("Multiple redirect matches for '{}'.".format(job_id))
            else:
                raise error


def _process_resource_project_api_version_1(project, o):
    head, tail = _split(o.path)

    if head == 'id':
        return project.get_id()
    elif head in ('len', 'num_jobs'):
        return len(project)
    elif head == 'job':
        job_id, path = _split(tail)
        job = _open_job_with_redirects(project, job_id)
        return _process_resource_job(job, path, o.query)
    elif head == 'jobs':
        return _process_resource_jobs(project, o.query)
    elif head == 'workspace':
        return os.path.join(project.workspace(), tail)
    elif head == 'root':
        return os.path.join(project.root_directory())
    elif head in ('doc', 'document'):
        return _process_resource_document(project.document, tail, o.query)
    elif head == 'schema':
        return _process_resource_schema(project, tail, o.query)
    elif head == 'index':
        return _process_resource_index(project, tail, o.query)
    elif head:
        raise InvalidRequestError("Unknown request '{}'.".format(head))
    else:
        return project


__all__ = ['InvalidRequestError', 'link_to', 'retrieve']
