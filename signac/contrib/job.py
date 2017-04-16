# Copyright (c) 2017 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
import os
import errno
import logging
import shutil
import copy

from ..common import six
from ..core.json import json
from ..core.jsondict import JSonDict
from ..core.attr_dict import AttrDict
from ..core.attr_dict import convert_to_dict
from .hashing import calc_id
from .utility import _mkdir_p
from .errors import DestinationExistsError

logger = logging.getLogger(__name__)


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
    FN_DOCUMENT = 'signac_job_document.json'
    "The job's document filename."

    def __init__(self, project, statepoint):
        self._project = project
        self._statepoint = json.loads(json.dumps(statepoint))
        self._id = calc_id(self._statepoint)
        self._document = None
        self._wd = os.path.join(project.workspace(), str(self))
        self._cwd = list()
        self._sp = None

    def get_id(self):
        """The unique identifier for the job's statepoint.

        :return: The job id.
        :rtype: str"""
        return self._id

    def __hash__(self):
        return hash(self._wd)

    def __str__(self):
        "Returns the job's id."
        return str(self.get_id())

    def __repr__(self):
        return "{}(project={}, statepoint={})".format(
            self.__class__.__module__ + '.' + self.__class__.__name__,
            repr(self._project), self._statepoint)

    def __eq__(self, other):
        return hash(self) == hash(other)

    def workspace(self):
        """Each job is associated with a unique workspace directory.

        :return: The path to the job's workspace directory.
        :rtype: str"""
        return self._wd

    @property
    def ws(self):
        "The job's workspace directory."
        return self.workspace()

    def statepoint(self):
        """The statepoint associated with this job.

        :return: The statepoint mapping.
        :rtype: dict"""
        return copy.deepcopy(self._statepoint)

    def reset_statepoint(self, new_statepoint):
        """Reset the state point of this job.

        .. danger::

            Use this function with caution! Resetting a job's state point,
            may sometimes be necessary, but can possibly lead to incoherent
            data spaces.

        :param new_statepoint: The job's new state point.
        :type new_statepoint: mapping
        :raises DestinationExistsError:
            If a job associated with the new state point is already initialized.
        :raises OSError:
            If the move failed due to an unknown system related error.
        """
        dst = self._project.open_job(new_statepoint)
        if dst == self:
            return
        fn_manifest = os.path.join(self.workspace(), self.FN_MANIFEST)
        fn_manifest_backup = fn_manifest + '~'
        try:
            os.rename(fn_manifest, fn_manifest_backup)
            try:
                os.rename(self.workspace(), dst.workspace())
            except OSError as error:
                os.rename(fn_manifest_backup, fn_manifest)  # rollback
                if error.errno == errno.ENOTEMPTY:
                    raise DestinationExistsError(dst)
                else:
                    raise
            else:
                dst.init()
        except OSError as error:
            if error.errno == errno.ENOENT:
                pass  # job is not initialized
            else:
                raise
        logger.info("Moved '{}' -> '{}'.".format(self, dst))
        dst._sp = self._sp
        self.__dict__.update(dst.__dict__)

    def _reset_sp(self, new_sp):
        self.reset_statepoint(convert_to_dict(new_sp))

    def update_statepoint(self, update, overwrite=False):
        """Update the statepoint of this job.

        .. warning::

            While appending to a job's state point is generally safe,
            modifying existing parameters may lead to data
            inconsistency. Use the overwrite argument with caution!

        :param update: A mapping used for the statepoint update.
        :type update: mapping
        :param overwrite:
            Set to true, to ignore whether this update overwrites parameters,
            which are currently part of the job's state point. Use with caution!
        :raises KeyError:
            If the update contains keys, which are already part of the job's
            state point and overwrite is False.
        :raises DestinationExistsError:
            If a job associated with the new state point is already initialized.
        :raises OSError:
            If the move failed due to an unknown system related error.
        """
        statepoint = self.statepoint()
        if not overwrite:
            for key, value in update.items():
                if statepoint.get(key, value) != value:
                    raise KeyError(key)
        statepoint.update(update)
        self.reset_statepoint(statepoint)

    @property
    def sp(self):
        "Access the job's state point as attribute dictionary."
        if self._sp is None:
            self._sp = AttrDict(self.statepoint(), self._reset_sp)
        return self._sp

    @sp.setter
    def sp(self, new_sp):
        self._reset_sp(new_sp)

    @property
    def document(self):
        """The document associated with this job.

        :return: The job document handle.
        :rtype: :class:`~.JSonDict`"""
        if self._document is None:
            self._create_directory()
            fn = os.path.join(self.workspace(), self.FN_DOCUMENT)
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
                if six.PY2:
                    # Adapted from: http://stackoverflow.com/questions/10978869/
                    if overwrite:
                        flags = os.O_CREAT | os.O_WRONLY
                    else:
                        flags = os.O_CREAT | os.O_WRONLY | os.O_EXCL
                    try:
                        fd = os.open(fn_manifest, flags)
                    except OSError as error:
                        if error.errno != errno.EEXIST:
                            raise
                    else:
                        with os.fdopen(fd, 'w') as file:
                            file.write(blob)
                else:
                    with open(fn_manifest, 'w' if overwrite else 'x') as file:
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
                    assert calc_id(json.loads(file.read())) == self._id
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
        try:
            shutil.rmtree(self.workspace())
        except OSError as error:
            if error.errno != errno.ENOENT:
                raise
        else:
            if self._document is not None:
                self._document.data.clear()
                self._document = None

    def move(self, project):
        """Move this job to project.

        This function will attempt to move this instance of job from
        its original project to a different project.

        :param project: The project to move this job to.
        :type project: :py:class:`~.project.Project`
        :raises DestinationExistsError: If the job is already initialized in project.
        """
        dst = project.open_job(self.statepoint())
        _mkdir_p(project.workspace())
        try:
            os.rename(self.workspace(), dst.workspace())
        except OSError:
            raise DestinationExistsError(dst)
        self.__dict__.update(dst.__dict__)

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

        .. code-block:: python

            with project.open_job(my_statepoint) as job:
                # manipulate your job data

        Opening the context will switch into the job's workspace,
        leaving it will switch back to the previous working directory.
        """
        self._cwd.append(os.getcwd())
        self._create_directory()
        logger.info("Enter workspace '{}'.".format(self.workspace()))
        os.chdir(self.workspace())

    def close(self):
        "Close the job and switch to the previous working directory."
        try:
            os.chdir(self._cwd.pop())
            logger.info("Leave workspace.")
        except IndexError:
            pass

    def __enter__(self):
        self.open()
        return self

    def __exit__(self, err_type, err_value, tb):
        self.close()
        return False
