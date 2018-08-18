# Copyright (c) 2018 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
from .project import Project


def link_to(job, start=None):
    """Create a link document for job.

    The link document can be used to lookup this job for instance to create
    one-to-one or one-to-many relationships across projects.

    :seealso: :func:`.lookup`
    """
    return job._as_dict(start=start)


def lookup(link, start=None):
    """Lookup jobs from link document.

    :seealso: :py:meth:`.Job.make_link`

    :param link:
        The document that links to the referenced job.
    :raises LookupError:
        If a project specified by a link cannot be found.
    :raises KeyError:
        If a job specified by a link does not exist.
    """
    return Project._lookup(link=link, start=start)
