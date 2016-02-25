import os
import re
import logging
import json
import glob
import errno
import collections

from ..common import six
from ..common.config import load_config
from .job import Job
from .hashing import calc_id

if six.PY2:
    from collections import Mapping
else:
    from collections.abc import Mapping

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
        """Returns the project's workspace directory.

        The workspace defaults to `project_root/workspace`.
        Configure this directory with the 'workspace_dir'
        attribute.
        If the specified directory is a relative path,
        the absolute path is relative from the project's
        root directory.

        .. note::
            The configuration will respect environment variables,
            such as $HOME."""
        wd = os.path.expandvars(self._config.get('workspace_dir', 'workspace'))
        if os.path.isabs(wd):
            return wd
        else:
            return os.path.join(self.root_directory(), wd)

    def get_id(self):
        """Get the project identifier.

        :return: The project id.
        :rtype: str
        :raises KeyError: If no project id could be determined.
        """
        try:
            return str(self.config['project'])
        except KeyError:
            msg = "Unable to determine project id. "
            msg += "Are you sure '{}' is a signac project path?"
            raise LookupError(msg.format(os.path.abspath(os.getcwd())))

    def open_job(self, statepoint=None, id=None):
        """Get a job handle associated with a statepoint.

        This function returns the job instance associated with
        the given statepoint or job id.
        Opening a job by statepoint never fails.
        Opening a job by id, requires a lookup of the statepoint
        from the job id, which may fail if the job was not
        previously initialized.

        :param statepoint: The job's unique set of parameters.
        :type statepoint: mapping
        :param id: The job id.
        :type id: str
        :return: The job instance.
        :rtype: :class:`signac.contrib.job.Job`
        :raises KeyError: If the attempt to open the job by id fails.
        """
        if (id is None) == (statepoint is None):
            raise ValueError(
                "You need to either provide the statepoint or the id.")
        if id is None:
            return Job(self, statepoint)
        else:
            try:
                return Job(self, self.get_statepoint(id))
            except KeyError as error:
                logger.warning(
                    "Unable to find statepoint for job id '{}' "
                    "Is the job initialized?".format(id))
                raise error

    def find_jobs(self, filter=None):
        """Find all jobs in the project's workspace.

        :param filter: If not None, only find jobs matching the filter.
        :type filter: mapping
        :yields: Instances of :class:`~signac.contrib.job.Job`"""
        for statepoint in self.find_statepoints(filter):
            yield Job(self, statepoint)

    def find_statepoints(self, filter=None, skip_errors=False):
        """Find all statepoints in the project's workspace.

        :param filter: If not None, only yield statepoints matching the filter.
        :type filter: mapping
        :param skip_errors: Show, but otherwise ignore errors while iterating
            over the workspace. Use this argument to repair a corrupted workspace.
        :type skip_erros: bool
        :yields: statepoints as dict"""
        filter = None if filter is None else json.loads(json.dumps(filter))
        def _match(doc, f):
            for key, value in f.items():
                if key not in doc or doc[key] != value:
                    return False
            return True
        wd = self.workspace()
        m = re.compile('[a-z0-9]{32}')
        job_dirs = [d for d in os.listdir(wd) if m.match(d)]
        for job_dir in job_dirs:
            fn_manifest = os.path.join(wd, job_dir, Job.FN_MANIFEST)
            try:
                with open(fn_manifest) as manifest:
                    statepoint = json.load(manifest)
                    if filter is None or _match(statepoint, filter):
                        yield statepoint
            except Exception as error:
                msg = "Error while trying to access manifest file: "\
                      "'{}'. Error: '{}'.".format(fn_manifest, error)
                logger.critical(msg)
                if not skip_errors:
                    raise error

    def read_statepoints(self, fn=None):
        """Read all statepoints from a file.

        :param fn: The filename of the file containing the statepoints,
            defaults to :const:`~signac.contrib.project.FN_STATEPOINTS`.
        :type fn: str

        See also :meth:`dump_statepoints`.
        See also :meth:`write_statepoints`.
        """
        if fn is None:
            fn = os.path.join(self.root_directory(), FN_STATEPOINTS)
        # See comment in write statepoints.
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

    def write_statepoints(self, statepoints=None, fn=None, indent=2):
        """Dump statepoints to a file.

        If the file already contains statepoints, all new statepoints
        will be appended, while the old ones are preserved.

        :param statepoints: A list of statepoints,
            defaults to all statepoints which are defined in the workspace.
        :type statepoints: iterable
        :param fn: The filename of the file containing the statepoints,
            defaults to :const:`~signac.contrib.project.FN_STATEPOINTS`.
        :type fn: str
        :param indent: Specify the indentation of the json file.
        :type indent: int

        See also :meth:`dump_statepoints`.
        """
        if fn is None:
            fn = os.path.join(self.root_directory(), FN_STATEPOINTS)
        try:
            tmp = self.read_statepoints(fn=fn)
        # except FileNotFoundError:
        except IOError as error:
            if not error.errno == errno.ENOENT:
                raise
            tmp = dict()
        if statepoints is None:
            statepoints = self.find_statepoints()
        tmp.update(self.dump_statepoints(statepoints))
        with open(fn, 'w') as file:
            file.write(json.dumps(tmp, indent=indent))

    def _get_statepoint_from_workspace(self, jobid):
        fn_manifest = os.path.join(self.workspace(), jobid, Job.FN_MANIFEST)
        try:
            with open(fn_manifest, 'r') as manifest:
                return json.load(manifest)
        except (IOError, ValueError) as error:
            if os.path.isfile(fn_manifest):
                msg = "Error while trying to access manifest file: "\
                      "'{}'. Error: '{}'.".format(fn_manifest, error)
                logger.critical(msg)
            raise KeyError(jobid)

    def get_statepoint(self, jobid, fn=None):
        """Get the statepoint associated with a job id.

        The statepoint is retrieved from the workspace or
        from the statepoints file if the former attempt fails.

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
        try:
            statepoint = self._get_statepoint_from_workspace(jobid)
        except KeyError:
            try:
                statepoint = self.read_statepoints(fn=fn)[jobid]
            except IOError as error:
                if not error.errno == errno.ENOENT:
                    raise
                raise KeyError(jobid)
        assert statepoint is not None
        assert str(self.open_job(statepoint)) == jobid
        return statepoint

    def create_view(self, filter=None, prefix='view'):
        """Create a view of the workspace.

        This function gathers all varying statepoint parameters
        and creates symbolic links to the workspace directories.
        This is useful for browsing through the workspace in a
        human-readable manner.


        Let's assume the parameter space is

            * a=0, b=0
            * a=1, b=0
            * a=2, b=0
            * ...,

        where *b* does not vary over all statepoints.

        Calling this method will generate the following *symbolic links* within
        the speciefied  view directory:

        .. code:: bash

            view/a/0 -> /path/to/workspace/7f9fb369851609ce9cb91404549393f3
            view/a/1 -> /path/to/workspace/017d53deb17a290d8b0d2ae02fa8bd9d
            ...

        .. note::

            As *b* does not vary over the whole parameter space it is not part
            of the view url.
            This maximizes the compactness of each view url.

        :param filter:  If not None,
            create view only for jobs matching filter.
        :type filter: mapping
        :param prefix: Specifies where to create the links."""
        statepoints = list(self.find_statepoints(filter=filter))
        if not len(statepoints):
            if filter is None:
                logger.warning("No state points found!")
            else:
                logger.warning("No state points matched the filter.")
        key_set = list(_find_unique_keys(statepoints))
        if filter is not None:
            key_set[:0] = [[key] for key in filter.keys()]
        for statepoint, url in _make_urls(statepoints, key_set):
            src = self.open_job(statepoint).workspace()
            dst = os.path.join(prefix, url)
            logger.info(
                "Creating link {src} -> {dst}".format(src=src, dst=dst))
            _make_link(src, dst)

    def find_job_documents(self, filter=None):
        """Find all job documents in the project's workspace.

        This method iterates through all jobs or all jobs matching
        the filter and yields each job's document as a dict.
        Each dict additionally contains a field 'statepoint',
        with the job's statepoint and a field '_id', which is
        the job's id.

        :param filter: If not None,
            only find job documents matching filter.
        :type filter: mapping
        :yields: Instances of dict.
        :raises KeyError: If the job document already contains the fields
            '_id' or 'statepoint'."""
        for job in self.find_jobs(filter=filter):
            doc = dict(job.document)
            if '_id' in doc:
                raise KeyError(
                    "The job document already contains a field '_id'!")
            if 'statepoint' in doc:
                raise KeyError(
                    "The job document already contains a field 'statepoint'!")
            doc['_id'] = str(job)
            doc['statepoint'] = job.statepoint()
            yield doc

    def reset_statepoint(self, job, new_statepoint):
        """Reset the statepoint of job.

        .. danger::

            Use this function with caution! Resetting a job's statepoint,
            may sometimes be necessary, but can possibly lead to incoherent
            data spaces.
            If you only want to *extend* your statepoint, consider to
            use :meth:`~.update_statepoint` instead.

        :param job: The job, that should be reset to a new state point.
        :type job: :class:`~.contrib.job.Job`
        :param new_statepoint: The job's new unique set of parameters.
        :type new_statepoint: mapping
        :raises RuntimeError: If a job associated with the new unique set
            of parameters already exists in the workspace."""
        dst = self.open_job(new_statepoint)
        _move_job(job, dst)
        logger.info(
            "Reset statepoint of job {}, moved to {}.".format(job, dst))

    def update_statepoint(self, job, update, overwrite=False):
        """Update the statepoint of job.

        .. warning::

            While appending to a job's statepoint is generally safe,
            modifying existing parameters may lead to data
            inconsistency. Use the overwrite argument with caution!

        :param job: The job, whose statepoint shall be updated.
        :type job: :class:`~.contrib.job.Job`
        :param update: A mapping used for the statepoint update.
        :type update: mapping
        :param overwrite: Set to true, to ignore whether this
            update overwrites parameters, which are currently
            part of the job's statepoint. Use with caution!
        :raises KeyError: If the update contains keys, which are
            already part of the job's statepoint.
        :raises RuntimeError: If a job associated with the new unique set
            of parameters already exists in the workspace."""
        statepoint = dict(job.statepoint())
        if not overwrite:
            for key in update:
                if key in statepoint:
                    raise KeyError(key)
        statepoint.update(update)
        _move_job(job, self.open_job(statepoint))

    def repair(self):
        "Attempt to repair the workspace after it got corrupted."
        for fn in glob.iglob(os.path.join(self.workspace(), '*')):
            jobid = os.path.split(fn)[-1]
            fn_manifest = os.path.join(fn, Job.FN_MANIFEST)
            try:
                with open(fn_manifest) as manifest:
                    statepoint = json.load(manifest)
            except Exception as error:
                logger.warning(
                    "Encountered error while reading from '{}'. "
                    "Error: {}".format(fn_manifest, error))
                try:
                    logger.info("Attempt to recover statepoint from file.")
                    statepoint = self.get_statepoint(jobid)
                    self.open_job(statepoint)._create_directory(overwrite=True)
                except KeyError as error:
                    raise RuntimeWarning(
                        "Use write_statepoints() before attempting to repair!")
                except IOError as error:
                    if FN_STATEPOINTS in str(error):
                        raise RuntimeWarning(
                            "Use write_statepoints() before attempting to repair!")
                    raise
                except Exception:
                    logger.error("Attemp to repair job space failed.")
                    raise
                else:
                    logger.info("Successfully recovered state point.")


def _move_job(src, dst):
    logger.debug("Attempting to move job {} to {}".format(src, dst))
    fn_src_manifest = os.path.join(src.workspace(), src.FN_MANIFEST)
    fn_src_manifest_backup = fn_src_manifest + '~'
    os.rename(fn_src_manifest, fn_src_manifest_backup)
    try:
        os.rename(src.workspace(), dst.workspace())
    except OSError:  # rollback
        os.rename(fn_src_manifest_backup, fn_src_manifest)
        raise RuntimeError(
            "Failed to move {} to {}, destination already exists.".format(
                src, dst))
    else:
        dst.init()
        logger.info("Moved job {} to {}.".format(src, dst))


def _make_link(src, dst):
    try:
        os.makedirs(os.path.dirname(dst))
    # except FileExistsError:
    except OSError as error:
        if error.errno != errno.EEXIST:
            raise
        pass
    if six.PY2:
        os.symlink(src, dst)
    else:
        os.symlink(src, dst, target_is_directory=True)


def _make_urls(statepoints, key_set):
    "Create unique URLs for all jobs matching filter."
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
        if len(url):
            yield statepoint, os.path.join(*url)


def _find_unique_keys(statepoints):
    key_set = _aggregate_statepoints(statepoints)
    if six.PY2:
        def flatten(l):
            for el in l:
                if isinstance(el, collections.Iterable) and not \
                        (isinstance(el, str) or isinstance(el, unicode)):  # noqa
                    for sub in flatten(el):
                        yield sub
                else:
                    yield el
    else:
        def flatten(l):
            for el in l:
                if isinstance(el, collections.Iterable) and \
                        not (isinstance(el, str)):
                    for sub in flatten(el):
                        yield sub
                else:
                    yield el
    key_set = (list(flatten(k)) for k in key_set)
    for key in sorted(key_set, key=len):
        yield key


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
                if isinstance(value, Mapping):
                    result.extend(_aggregate_statepoints(
                        [sp[key] for sp in statepoints if key in sp],
                        prefix=(key) if prefix is None else (prefix, key)))
                    ignore.add(key)
                else:
                    statepoint_set[key].add(calc_id(value))
    # Heal heterogenous parameter space.
    for statepoint in statepoints:
        for key in statepoint_set.keys():
            if key not in statepoint:
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
