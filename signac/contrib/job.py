# Copyright (c) 2016 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
import os
import errno
import logging
import json
import shutil
import copy

from ..common import six
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
        self._statepoint = json.loads(json.dumps(statepoint))
        self._id = calc_id(self._statepoint)
        self._document = None
        self._wd = os.path.join(project.workspace(), str(self))
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
        return copy.deepcopy(self._statepoint)

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

    def _create_directory(self, overwrite=False):
        "Create the workspace directory and write the manifest file."
        fn_manifest = os.path.join(self.workspace(), self.FN_MANIFEST)

        # Create the workspace directory if it did not exist yet.
        _mkdir_p(self.workspace())

        try:
            # Ensure to create the binary to write before file creation
            blob = json.dumps(self.statepoint(), indent=2)

            try:
                # Open the file for writing only if it does not exist yet.
                mode = 'w' if overwrite else 'wx' if six.PY2 else 'x'
                with open(fn_manifest, mode) as file:
                    file.write(blob)
            except IOError as error:
                if not error.errno == errno.EEXIST:
                    raise
        except Exception as error:
            # Attempt to delete the file on error, to prevent corruption.
            try:
                os.remove(fn_manifest)
            except Exception:  # ignore all errors here
                pass
            raise error
        else:
            self._check_manifest()

    def _check_manifest(self):
        "Check whether the manifest file, if it exists, is correct."
        fn_manifest = os.path.join(self.workspace(), self.FN_MANIFEST)
        try:
            try:
                with open(fn_manifest) as file:
                    assert calc_id(json.load(file)) == self._id
            except IOError as error:
                if not error.errno == errno.ENOENT:
                    raise error
        except Exception as error:
            msg = "Manifest file of job '{}' is corrupted: {}."
            raise RuntimeError(msg.format(self, error))

    def init(self):
        """Initialize the job's workspace directory.

        This function will do nothing if the directory and
        the job manifest already exist."""
        self._create_directory()

    def remove(self):
        """Remove the job's workspace including the job document.

        This function will do nothing if the workspace directory
        does not exist."""
        if self._document is not None:
            self._document.clear()
        try:
            shutil.rmtree(self.workspace())
        except OSError as error:
            if error.errno != errno.ENOENT:
                raise

    def fn(self, filename):
        """Prepend a filename with the job's workspace directory path.

        :param filename: The filename of the file.
        :type filename: str
        :return: The full workspace path of the file."""
        return os.path.join(self.workspace(), filename)

    def isfile(self, filename):
        """Return True if file exists in the job's workspace.

        :param filename: The filename of the file.
        :type filename: str
        :return: True if file with filename exists in workspace.
        :rtype: bool"""
        return os.path.isfile(self.fn(filename))

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
