import os
import logging
import json
import glob
import collections
import collections.abc
import itertools

from ..common.config import load_config
from .job import Job
from .hashing import calc_id

logger = logging.getLogger(__name__)

#: The default filename to read from and write statepoints to.
FN_STATEPOINTS = 'signac_statepoints.json'


class Project(object):
    """The handle on a signac project.

    Application developers should usually not need to
    directly instantiate this class, but use
    :func:`.contrib.get_project` instead."""

    def __init__(self, config=None):
        if config is None:
            config = load_config()
        self._config = config
        self.get_id()

    def __str__(self):
        "Returns the project's id."
        return str(self.get_id())

    @property
    def config(self):
        "The project's configuration."
        return self._config

    def root_directory(self):
        "Returns the project's root directory."
        return self._config['project_dir']

    def workspace(self):
        "Returns the project's workspace directory."
        return self._config['workspace_dir']

    def get_id(self):
        """Get the project identifier.

        :return: The project id.
        :rtype: str
        :raises: KeyError if no project id could be determined.
        """
        try:
            return str(self.config['project'])
        except KeyError:
            msg = "Unable to determine project id. "
            msg += "Are you sure '{}' is a signac project path?"
            raise LookupError(msg.format(os.path.abspath(os.getcwd())))

    def open_job(self, statepoint):
        """Get a job handle associated with statepoint.

        :param statepoint: The job's unique set of parameters.
        :type statepoint: mapping
        :return: The job instance.
        :rtype: :class:`signac.contrib.job.Job`
        """
        return Job(self, statepoint)

    def find_jobs(self, filter=None):
        """Find all jobs in the project's workspace.

        :param filter: If not None, only find jobs matching the filter.
        :type filter: mapping
        :yields: Instances of :class:`~signac.contrib.job.Job`"""
        for statepoint in self.find_statepoints(filter=filter):
            yield Job(self, statepoint)


    def find_statepoints(self, filter=None):
        "Find all statepoints in the project's workspace."
        def _match(doc, f):
            for key, value in f.items():
                if not key in doc or doc[key] != value:
                    return False
            return True
        for fn_manifest in glob.iglob(os.path.join(
                self.workspace(), '*', Job.FN_MANIFEST)):
            with open(fn_manifest) as manifest:
                statepoint = json.load(manifest)
                if filter is None or _match(statepoint, filter):
                    yield statepoint



    def read_statepoints(self, fn=None):
        """Read all statepoints from a file.

        :param fn: The filename of the file containing the statepoints,
            defaults to :const:`~signac.contrib.project.FN_STATEPOINTS`.
        :type fn: str

        See also :meth:`dump_statepoints`.
        """
        if fn is None:
            fn = os.path.join(self.root_directory(), FN_STATEPOINTS)
        with open(fn, 'r') as file:
            return json.loads(file.read())

    def dump_statepoints(self, statepoints):
        """Dump the statepoints and associated job ids.

        Equivalent to:

        .. code::

            {project.open_job(sp).get_id(): sp for sp in statepoints}

        :param statepoints: A list of statepoints.
        :type statepoints: iterable
        :return: A mapping, where the key is the job id
                 and the value is the statepoint.
        :rtype: dict
        """
        return {calc_id(sp): sp for sp in statepoints}

    def write_statepoints(self, statepoints, fn=None):
        """Dump statepoints to a file.

        If the file already contains statepoints, all new statepoints
        will be appended, while the old ones are preserved.

        :param statepoints: A list of statepoints.
        :type statepoints: iterable
        :param fn: The filename of the file containing the statepoints,
            defaults to :const:`~signac.contrib.project.FN_STATEPOINTS`.
        :type fn: str

        See also :meth:`dump_statepoints`.
        """
        if fn is None:
            fn = os.path.join(self.root_directory(), FN_STATEPOINTS)
        try:
            tmp = self.read_statepoints(fn=fn)
        except FileNotFoundError:
            tmp = dict()
        tmp.update(self.dump_statepoints(statepoints))
        with open(fn, 'w') as file:
            file.write(json.dumps(tmp))

    def get_statepoint(self, jobid, fn=None):
        """Get the statepoint associated with a job id.

        Reads the statepoints file and returns the statepoint.

        :param jobid: A job id to get the statepoint for.
        :type jobid: str
        :param fn: The filename of the file containing the statepoints,
            defaults to :const:`~signac.contrib.project.FN_STATEPOINTS`.
        :type fn: str
        :return: The statepoint.
        :rtype: dict
        :raises KeyError: If the statepoint associated with \
            jobid could not be found.

        See also :meth:`dump_statepoints`.
        """
        return self.read_statepoints(fn=fn)[jobid]

    def create_view(self, filter=None, prefix='.', prefix_filter=True):
        """Create a view of the workspace."""
        if prefix_filter and filter is not None:
            prefix = os.path.join(prefix, *(os.path.join(str(k), str(v)) for k, v in filter.items()))
        statepoints = list(self.find_statepoints(filter=filter))
        for statepoint, url in _make_urls(statepoints):
            src = self.open_job(statepoint).workspace()
            dst = os.path.join(prefix, url)
            _make_link(src, dst)


def _make_link(src, dst):
    try:
        os.makedirs(os.path.dirname(dst))
    except FileExistsError:
        pass
    os.symlink(src, dst, target_is_directory=True)


def _make_urls(statepoints):
    "Create unique URLs for all jobs matching filter."
    key_set = list(_find_unique_keys(statepoints))
    for statepoint in statepoints:
        url = []
        for keys in key_set:
            url.append('.'.join(keys))
            v = statepoint
            for key in keys:
                v = v.get(key)
                if v is None:
                    break
            url.append(str(v))
        yield statepoint, os.path.join(*url)


def _find_unique_keys(statepoints):
    key_set = _aggregate_statepoints(statepoints)
    def flatten(l):
        for el in l:
            if isinstance(el, collections.Iterable) and not isinstance(el, str):
                for sub in flatten(el):
                    yield sub
            else:
                yield el
    key_set = (list(flatten(k)) for k in key_set)
    key_set = yield from sorted(key_set, key=len)

def _aggregate_statepoints(statepoints, prefix=None):
    result = list()
    statepoint_set = collections.defaultdict(set)
    # Gather all keys.
    ignore = set()
    for statepoint in statepoints:
        for key, value in statepoint.items():
            if key in ignore:
                continue
            try:
                statepoint_set[key].add(value)
            except TypeError:
                if isinstance(value, collections.abc.Mapping):
                    result.extend(_aggregate_statepoints(
                        [sp[key] for sp in statepoints if key in sp],
                        prefix = (key) if prefix is None else (prefix, key)))
                    ignore.add(key)
                else:
                    statepoint_set[key].add(calc_id(value))
    # Heal heterogenous parameter space.
    for statepoint in statepoints:
        for key in statepoint_set.keys():
            if not key in statepoint:
                statepoint_set[key].add(None)
    unique_keys = list(k for k, v in sorted(
        statepoint_set.items(), key=lambda i: len(i[1])) if len(v) > 1)
    result.extend((k,) if prefix is None else (prefix, k) for k in unique_keys)
    return result


def get_project():
    """Find a project configuration and return the associated project.

    :returns: The project handle.
    :rtype: :class:`Project`"""
    return Project()
