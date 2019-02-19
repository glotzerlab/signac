# Copyright (c) 2017 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
import os
import errno
import logging
import shutil
import uuid

from ..common import six
from ..core.json import json, CustomJSONEncoder
from ..core.attrdict import SyncedAttrDict
from ..core.jsondict import JSONDict
from ..core.h5store import H5StoreManager
from .hashing import calc_id
from .utility import _mkdir_p
from .errors import DestinationExistsError, JobsCorruptedError
from ..sync import sync_jobs
if six.PY2:
    from collections import Mapping
else:
    from collections.abc import Mapping

logger = logging.getLogger(__name__)


class _sp_save_hook(object):

    def __init__(self, job):
        self.job = job

    def load(self):
        pass

    def save(self):
        self.job._reset_sp()


class Job(object):
    """The job instance is a handle to the data of a unique statepoint.

    Application developers should usually not need to directly
    instantiate this class, but use :meth:`~signac.Project.open_job`
    instead."""

    FN_MANIFEST = 'signac_statepoint.json'
    """The job's manifest filename.

    The job manifest, this means a human-readable dump of the job's\
    statepoint is stored in each workspace directory.
    """

    FN_DOCUMENT = 'signac_job_document.json'
    "The job's document filename."

    KEY_DATA = 'signac_data'

    def __init__(self, project, statepoint, _id=None):
        self._project = project

        # Ensure that the job id is configured
        if _id is None:
            self._statepoint = json.loads(json.dumps(statepoint, cls=CustomJSONEncoder))
            self._id = calc_id(self._statepoint)
        else:
            self._statepoint = dict(statepoint)
            self._id = _id

        # Prepare job statepoint
        self._sp = SyncedAttrDict(self._statepoint, parent=_sp_save_hook(self))

        # Prepare job working directory
        self._wd = os.path.join(project.workspace(), self._id)

        # Prepare job document
        self._fn_doc = os.path.join(self._wd, self.FN_DOCUMENT)
        self._document = None

        # Prepare job h5-stores
        self._stores = H5StoreManager(self._wd)

        # Prepare current working directory for context management
        self._cwd = list()

    def get_id(self):
        """The unique identifier for the job's statepoint.

        :return: The job id.
        :rtype: str"""
        return self._id

    def __hash__(self):
        return hash(os.path.realpath(self._wd))

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
        """Alias for :attr:`Job.workspace`."""
        return self.workspace()

    def reset_statepoint(self, new_statepoint):
        """Reset the state point of this job.

        .. danger::

            Use this function with caution! Resetting a job's state point
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
        fn_manifest = os.path.join(self._wd, self.FN_MANIFEST)
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
        # Update this instance
        self._statepoint = dst._statepoint
        self._id = dst._id
        self._sp = SyncedAttrDict(self._statepoint, parent=_sp_save_hook(self))
        self._wd = dst._wd
        self._fn_doc = dst._fn_doc
        self._document = None
        self._data = None
        self._cwd = list()
        logger.info("Moved '{}' -> '{}'.".format(self, dst))

    def _reset_sp(self, new_sp=None):
        if new_sp is None:
            new_sp = self.statepoint()
        self.reset_statepoint(new_sp)

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
    def statepoint(self):
        """Access the job's state point as attribute dictionary.
        """
        if self._sp is None:
            self._sp = SyncedAttrDict(self._statepoint, parent=_sp_save_hook(self))
        return self._sp

    @statepoint.setter
    def statepoint(self, new_sp):
        self._reset_sp(new_sp)

    @property
    def sp(self):
        """ Alias for :attr:`Job.statepoint`.
        """
        return self.statepoint

    @sp.setter
    def sp(self, new_sp):
        self.statepoint = new_sp

    def _reset_document(self, new_doc):
        if not isinstance(new_doc, Mapping):
            raise ValueError("The document must be a mapping.")
        dirname, filename = os.path.split(self._fn_doc)
        fn_tmp = os.path.join(dirname, '._{uid}_{fn}'.format(
            uid=uuid.uuid4(), fn=filename))
        with open(fn_tmp, 'wb') as tmpfile:
            tmpfile.write(json.dumps(new_doc, cls=CustomJSONEncoder).encode())
        if six.PY2:
            os.rename(fn_tmp, self._fn_doc)
        else:
            os.replace(fn_tmp, self._fn_doc)

    @property
    def document(self):
        """The document associated with this job.

        :return: The job document handle.
        :rtype: :class:`~.JSONDict`"""
        if self._document is None:
            self.init()
            self._document = JSONDict(filename=self._fn_doc, write_concern=True)
        return self._document

    @document.setter
    def document(self, new_doc):
        self._reset_document(new_doc)

    @property
    def doc(self):
        """Alias for :attr:`Job.document`.
        """
        return self.document

    @doc.setter
    def doc(self, new_doc):
        self.document = new_doc

    @property
    def stores(self):
        return self.init()._stores

    @property
    def data(self):
        """The data associated with this job.

        Equivalent to:

            return job.store['signac_data']

        :return: An HDF5-backed datastore.
        :rtype: :class:`~signac.core.h5store.H5Store`"""
        return self.stores[self.KEY_DATA]

    @data.setter
    def data(self, new_data):
        self.stores[self.KEY_DATA] = new_data

    def _init(self, force=False):
        fn_manifest = os.path.join(self._wd, self.FN_MANIFEST)

        # Create the workspace directory if it did not exist yet.
        try:
            _mkdir_p(self._wd)
        except OSError:
            logger.error("Error occured while trying to create "
                         "workspace directory for job '{}'.".format(self))
            raise

        try:
            # Ensure to create the binary to write before file creation
            blob = json.dumps(self._statepoint, indent=2, cls=CustomJSONEncoder)

            try:
                # Open the file for writing only if it does not exist yet.
                if six.PY2:
                    # Adapted from: http://stackoverflow.com/questions/10978869/
                    if force:
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
                    with open(fn_manifest, 'w' if force else 'x') as file:
                        file.write(blob)
            except (IOError, OSError) as error:
                if error.errno not in (errno.EEXIST, errno.EACCES):
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
        fn_manifest = os.path.join(self._wd, self.FN_MANIFEST)
        try:
            with open(fn_manifest, 'rb') as file:
                assert calc_id(json.loads(file.read().decode())) == self._id
        except IOError as error:
            if error.errno != errno.ENOENT:
                raise error
        except (AssertionError, ValueError):
            raise JobsCorruptedError([self._id])

    def init(self, force=False):
        """Initialize the job's workspace directory.

        This function will do nothing if the directory and
        the job manifest already exist.

        Returns the calling job.

        :param force:
            Overwrite any existing state point's manifest
            files, e.g., to repair them when they got corrupted.
        :type force:
            bool
        :return:
            The job handle.
        :rtype:
            :class:`~.Job`
        """
        try:
            self._init(force=force)
        except Exception:
            logger.error(
                "State point manifest file of job '{}' appears to be corrupted.".format(self._id))
            raise
        return self

    def clear(self):
        """Remove all job data, but not the job itself.

        This function will do nothing if the job was not previously
        initialized.
        """
        try:
            for fn in os.listdir(self._wd):
                if fn in (self.FN_MANIFEST, self.FN_DOCUMENT):
                    continue
                path = os.path.join(self._wd, fn)
                if os.path.isfile(path):
                    os.remove(path)
                elif os.path.isdir(path):
                    shutil.rmtree(path)
            self.document.clear()
        except (OSError, IOError) as error:
            if error.errno != errno.ENOENT:
                raise error

    def reset(self):
        """Remove all job data, but not the job itself.

        This function will initialize the job if it was not previously
        initialized."""
        self.clear()
        self.init()

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
                try:
                    self._document.clear()
                except IOError as error:
                    if not error.errno == errno.ENOENT:
                        raise error
                self._document = None
            self._data = None

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

    def sync(self, other, strategy=None, exclude=None, doc_sync=None, **kwargs):
        """Perform a one-way synchronization of this job with the other job.

        By default, this method will synchronize all files and document data with
        the other job to this job until a synchronization conflict occurs. There
        are two different kinds of synchronization conflicts:

            1. The two jobs have files with the same, but different content.
            2. The two jobs have documents that share keys, but those keys are
               associated with different values.

        A file conflict can be resolved by providing a 'FileSync' *strategy* or by
        *excluding* files from the synchronization. An unresolvable conflict is indicated with
        the raise of a :py:class:`~.errors.FileSyncConflict` exception.

        A document synchronization conflict can be resolved by providing a doc_sync function
        that takes the source and the destination document as first and second argument.

        :param other:
            The other job to synchronize from.
        :type other:
            `.Job`
        :param strategy:
            A synchronization strategy for file conflicts. If no strategy is provided, a
            :class:`~.errors.SyncConflict` exception will be raised upon conflict.
        :param exclude:
            An filename exclude pattern. All files matching this pattern will be
            excluded from synchronization.
        :type exclude:
            str
        :param doc_sync:
            A synchronization strategy for document keys. If this argument is None, by default
            no keys will be synchronized upon conflict.
        :param dry_run:
            If True, do not actually perform the synchronization.
        :param kwargs:
            Extra keyword arguments will be forward to the :py:func:`~.sync.sync_jobs`
            function which actually excutes the synchronization operation.
        :raises FileSyncConflict:
            In case that a file synchronization results in a conflict.
        """
        sync_jobs(
            src=other,
            dst=self,
            strategy=strategy,
            exclude=exclude,
            doc_sync=doc_sync,
            **kwargs)

    def fn(self, filename):
        """Prepend a filename with the job's workspace directory path.

        :param filename: The filename of the file.
        :type filename: str
        :return: The full workspace path of the file."""
        return os.path.join(self._wd, filename)

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
        self.init()
        logger.info("Enter workspace '{}'.".format(self._wd))
        os.chdir(self._wd)

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
