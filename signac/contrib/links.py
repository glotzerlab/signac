# Copyright (c) 2018 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
from .project import Project


def link_to(project_or_job, start=None):
    """Create a link for a project or job.

    The link can be used to lookup this instance of project or job.

    :seealso: :func:`.lookup`
    """
    return project_or_job._as_link(start=start)


def lookup(link, start=None):
    """Lookup a project or job from the provided link.

    :seealso: :py:func:`.link_to`

    :param link:
        The URL that links to the referenced project or job.
    :raises LookupError:
        If a project specified by a link cannot be found.
    :raises KeyError:
        If a job specified by a link does not exist.
    """
    return Project._lookup(link=link, start=start)
