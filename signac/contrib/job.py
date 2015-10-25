import os
import errno
import logging
import json

from ..core.jsondict import JSonDict
from .hashing import calc_id

logger = logging.getLogger(__name__)


def _mkdir_p(path):
    try:
        os.makedirs(path)
    except OSError as error:
        if not (error.errno == errno.EEXIST and os.path.isdir(path)):
            raise


class Job(object):
    """The job instance is a handle to the data of a unique statepoint.

    Application developers should usually not need to directly
    instantiate this class, but use :meth:`~.project.Project.open_job`
    instead."""
    FN_MANIFEST = 'signac_statepoint.json'
    """The job's manifest filename.

    The job manifest, this means a human-readable dump of the job's\
    statepoint is stored in each workspace directory.
    """

    def __init__(self, project, statepoint):
        self._project = project
        self._statepoint = statepoint
        self._id = calc_id(statepoint)
        self._document = None
        self._wd = os.path.join(self._project.config[
                                'workspace_dir'], str(self))
        self._cwd = None

    def get_id(self):
        """The unique identifier for the job's statepoint.

        :return: The job id.
        :rtype: str"""
        return self._id

    def __str__(self):
        "Returns the job's id."
        return str(self.get_id())

    def workspace(self):
        """Each job is associated with a unique workspace directory.

        :return: The path to the job's workspace directory.
        :rtype: str"""
        return self._wd

    def statepoint(self):
        """The statepoint associated with this job.

        :return: The statepoint mapping.
        :rtype: dict"""
        return dict(self._statepoint)

    @property
    def document(self):
        """The document associated with this job.

        :return: The job document handle.
        :rtype: :class:`~.JSonDict`"""
        if self._document is None:
            self._create_directory()
            fn = os.path.join(self.workspace(), 'signac_job_document.json')
            self._document = JSonDict(
                fn, synchronized=True, write_concern=True)
        return self._document

    def _create_directory(self):
        "Create the workspace directory and write the manifest file."
        _mkdir_p(self.workspace())
        with open(os.path.join(self.workspace(),
                               self.FN_MANIFEST), 'w') as file:
            file.write(json.dumps(self.statepoint()))

    def open(self):
        """Enter the job's workspace directory.

        You can use the :class:`~.Job` class as context manager:

        .. code::

            with project.open_job(my_statepoint) as job:
                # manipulate your job data

        Opening the context will switch into the job's workspace,
        leaving it will switch back to the previous working directory.
        """
        if self._cwd is None:
            self._cwd = os.getcwd()
            self._create_directory()
            logger.info("Enter workspace '{}'.".format(self.workspace()))
            os.chdir(self.workspace())
        else:
            logger.debug("Job is already opened, doing nothing.")

    def close(self):
        "Close the job and switch to the previous working directory."
        logger.info("Leave workspace.")
        os.chdir(self._cwd)
        self._cwd = None

    def __enter__(self):
        self.open()
        return self

    def __exit__(self, err_type, err_value, tb):
        self.close()
        return False
