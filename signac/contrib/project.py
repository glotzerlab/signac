import os
import logging
import json

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
    :func:`.get_project` instead."""

    def __init__(self, config=None):
        if config is None:
            config = load_config()
        self._config = config

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
            msg += "Are you sure '{}' is a compDB project path?"
            raise LookupError(msg.format(os.path.abspath(os.getcwd())))

    def open_job(self, statepoint):
        """Get a job handle associated with statepoint.

        :param statepoint: The job's unique set of parameters.
        :type statepoint: mapping
        :return: The job instance.
        :rtype: :class:`signac.contrib.job.Job`
        """
        return Job(self, statepoint)

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


def get_project():
    """Find a project configuration and return the associated project.

    :returns: The project handle.
    :rtype: Instance of :class:`Project`."""
    return Project()
