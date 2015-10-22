import os
import logging
import json

from ..common.config import load_config
from .job import Job
from .hashing import calc_id

logger = logging.getLogger(__name__)

FN_STATEPOINTS = 'signac_statepoints.json'


class Project(object):

    def __init__(self, config=None):
        """Initializes a Project instance.

        Application developers should usually not need to
        instantiate this class.

        See ``Project`` instead.
        """
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
        """"Returns the project's id as determined from the configuration.

        :returns: str - The project id.
        :raises: KeyError

        This method raises ``KeyError`` if no project id could be determined.
        """
        try:
            return str(self.config['project'])
        except KeyError:
            msg = "Unable to determine project id. "
            msg += "Are you sure '{}' is a compDB project path?"
            raise LookupError(msg.format(os.path.abspath(os.getcwd())))

    def open_job(self, statepoint):
        return Job(self, statepoint)

    def read_statepoints(self, fn=None):
        if fn is None:
            fn = os.path.join(self.root_directory(), FN_STATEPOINTS)
        with open(fn, 'r') as file:
            return json.loads(file.read())

    def dump_statepoints(self, statepoints):
        return {calc_id(sp): sp for sp in statepoints}

    def write_statepoints(self, statepoints, fn=None):
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
        return self.read_statepoints(fn=fn)[jobid]


def get_project():
    return Project()
