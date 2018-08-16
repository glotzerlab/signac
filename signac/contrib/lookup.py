from .project import Project


def lookup(link, ignore_missing=False, start=None):
    """Lookup jobs from link documents.

    :seealso: :py:meth:`.Job.make_link`

    :param links:
        The documents that link to the individual jobs.
    :param init:
        Initialize jobs from links if necessary.
    :type init:
        bool
    :raises LookupError:
        If a project specified by a link cannot be found.
    :raises KeyError:
        If a job specified by a link does not exist, unless
        the init argument is set to True.
    """
    return Project._lookup_job(link, ignore_missing=ignore_missing, start=start)
