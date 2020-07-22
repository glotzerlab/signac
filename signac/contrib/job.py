# Copyright (c) 2017 The Regents of the University of Michigan.
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Job class defined here."""

import os
import errno
import logging
import shutil
from copy import deepcopy
from deprecation import deprecated

from ..core import json
from ..core.attrdict import SyncedAttrDict
from ..core.jsondict import JSONDict
from ..core.h5store import H5StoreManager
from .hashing import calc_id
from .utility import _mkdir_p
from .errors import DestinationExistsError, JobsCorruptedError
from ..sync import sync_jobs
from ..version import __version__


logger = logging.getLogger(__name__)


class _sp_save_hook(object):
    """Hook to handle job migration when statepoints are changed.

    When a job's state point is changed, in addition
    to the contents of the file being modified this hook
    calls :meth:`~Job._reset_sp` to rehash the state
    point, compute a new job id, and move the folder.

    Parameters
    ----------
    jobs : iterable of `Jobs`
        List of jobs(instance of `Job`).

    """
    def __init__(self, *jobs):
        self.jobs = list(jobs)

    def load(self):
        pass

    def save(self):
        """Reset the state point for all the jobs."""
        for job in self.jobs:
            job._reset_sp()


class Job(object):
    """The job instance is a handle to the data of a unique state point.

    Application developers should usually not need to directly
    instantiate this class, but use :meth:`~signac.Project.open_job`
    instead.

    Parameters
    ----------
    project : :class:`~signac.Project`
        Project handle.

    statepoint : dict
        State point for the job.

    _id : str
        A file-like object to write to.

    """
    FN_MANIFEST = 'signac_statepoint.json'
    """The job's manifest filename.

    The job manifest, this means a human-readable dump of the job's
    state point is stored in each workspace directory.
    """

    FN_DOCUMENT = 'signac_job_document.json'
    "The job's document filename."

    KEY_DATA = 'signac_data'
    "The job's datastore key."

    def __init__(self, project, statepoint, _id=None):
        self._project = project

        # Set statepoint and id
        self._statepoint = SyncedAttrDict(statepoint, parent=_sp_save_hook(self))
        self._id = calc_id(self._statepoint()) if _id is None else _id

        # Prepare job working directory
        self._wd = os.path.join(project.workspace(), self._id)

        # Prepare job document
        self._fn_doc = os.path.join(self._wd, self.FN_DOCUMENT)
        self._document = None

        # Prepare job h5-stores
        self._stores = H5StoreManager(self._wd)

        # Prepare current working directory for context management
        self._cwd = list()

    @deprecated(deprecated_in="1.3", removed_in="2.0", current_version=__version__,
                details="Use job.id instead.")
    def get_id(self):
        """Job's state point unique identifier.

        Returns
        -------
        str
            The job id.

        """
        return self._id

    @property
    def id(self):
        """Get the unique identifier for the job's state point.

        Returns
        -------
        str
            The job id.

        """
        return self._id

    def __hash__(self):
        return hash(os.path.realpath(self._wd))

    def __str__(self):
        """Return the job's id."""
        return str(self.id)

    def __repr__(self):
        return "{}(project={}, statepoint={})".format(
            self.__class__.__name__,
            repr(self._project), self._statepoint)

    def __eq__(self, other):
        return hash(self) == hash(other)

    def workspace(self):
        """Each job is associated with a unique workspace directory.

        Returns
        -------
        str
            The path to the job's workspace directory.

        """
        return self._wd

    @property
    def ws(self):
        """Alias for :meth:`~Job.workspace`."""
        return self.workspace()

    def reset_statepoint(self, new_statepoint):
        """Reset the state point of this job.

        .. danger::

            Use this function with caution! Resetting a job's state point
            may sometimes be necessary, but can possibly lead to incoherent
            data spaces.

        Parameters
        ----------
        new_statepoint : dict
            The job's new state point.

        """
        dst = self._project.open_job(new_statepoint)
        if dst == self:
            return
        fn_manifest = os.path.join(self._wd, self.FN_MANIFEST)
        fn_manifest_backup = fn_manifest + '~'
        try:
            os.replace(fn_manifest, fn_manifest_backup)
            try:
                os.replace(self.workspace(), dst.workspace())
            except OSError as error:
                os.replace(fn_manifest_backup, fn_manifest)  # rollback
                if error.errno in (errno.ENOTEMPTY, errno.EACCES):
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
        self._statepoint._data = dst._statepoint._data
        self._id = dst._id
        self._wd = dst._wd
        self._fn_doc = dst._fn_doc
        self._document = None
        self._data = None
        self._cwd = list()
        logger.info("Moved '{}' -> '{}'.".format(self, dst))

    def _reset_sp(self, new_sp=None):
        """Check for new state point requested to assign this job.

        Parameters
        ----------
        new_sp : dict
            The job's new state point (Default value = None).

        """
        if new_sp is None:
            new_sp = self.statepoint()
        self.reset_statepoint(new_sp)

    def update_statepoint(self, update, overwrite=False):
        """Update the state point of this job.

        .. warning::

            While appending to a job's state point is generally safe,
            modifying existing parameters may lead to data
            inconsistency. Use the overwrite argument with caution!

        Parameters
        ----------
        update : dict
            A mapping used for the state point update.
        overwrite :
            Set to true, to ignore whether this update overwrites parameters,
            which are currently part of the job's state point.
            Use with caution! (Default value = False)

        Raises
        ------
        KeyError
            If the update contains keys, which are already part of the job's
            state point and overwrite is False.
        DestinationExistsError
            If a job associated with the new state point is already initialized.
        OSError
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
        """Get the job's state point.

        .. warning::

            The state point object behaves like a dictionary in most cases,
            but because it persists changes to the filesystem, making a copy
            requires explicitly converting it to a dict. If you need a
            modifiable copy that will not modify the underlying JSON file,
            you can access a dict copy of the state point by calling it, e.g.
            ``sp_dict = job.statepoint()`` instead of ``sp = job.statepoint``.
            For more information, see : :class:`~signac.JSONDict`.

        Returns
        -------
        dict
            Returns the job's state point.

        """
        return self._statepoint

    @statepoint.setter
    def statepoint(self, new_sp):
        """Assign a new state point to this job.

        Parameters
        ----------
        new_sp : dict
            The new state point to be assigned.

        """
        self._reset_sp(new_sp)

    @property
    def sp(self):
        """Alias for :attr:`~Job.statepoint`."""
        return self.statepoint

    @sp.setter
    def sp(self, new_sp):
        """Alias for :attr:`~Job.statepoint`."""
        self.statepoint = new_sp

    @property
    def document(self):
        """Get document associated with this job.

        .. warning::

            If you need a deep copy that will not modify the underlying
            persistent JSON file, use :attr:`~Job.document` instead of :attr:`~Job.doc`.
            For more information, see :attr:`~Job.statepoint` or :class:`~signac.JSONDict`.

        Returns
        -------
        :class:`~signac.JSONDict`
            The job document handle.

        """
        if self._document is None:
            self.init()
            self._document = JSONDict(filename=self._fn_doc, write_concern=True)
        return self._document

    @document.setter
    def document(self, new_doc):
        """Assign new document to the this job.

        Parameters
        ----------
        new_doc : :class:`~signac.JSONDict`
            The job document handle.

        """
        self.document.reset(new_doc)

    @property
    def doc(self):
        """Alias for :attr:`~Job.document`.

        .. warning::

            If you need a deep copy that will not modify the underlying
            persistent JSON file, use :attr:`~Job.document` instead of :attr:`~Job.doc`.
            For more information, see :attr:`~Job.statepoint` or :class:`~signac.JSONDict`.

        """
        return self.document

    @doc.setter
    def doc(self, new_doc):
        """Alias for :attr:`~Job.document`."""
        self.document = new_doc

    @property
    def stores(self):
        """Get HDF5 stores associated with this job.

        Use this property to access an HDF5 file within the job's workspace
        directory using the :class:`~signac.H5Store` dict-like interface.

        This is an example for accessing an HDF5 file called 'my_data.h5' within
        the job's workspace:

        .. code-block:: python

            job.stores['my_data']['array'] = np.random((32, 4))

        This is equivalent to:

        .. code-block:: python

            H5Store(job.fn('my_data.h5'))['array'] = np.random((32, 4))

        Both the :attr:`~job.stores` and the :class:`~signac.H5Store` itself support attribute
        access. The above example could therefore also be expressed as:

        .. code-block:: python

            job.stores.my_data.array = np.random((32, 4))

        Returns
        -------
        :class:`~signac.H5StoreManager`
            The HDF5-Store manager for this job.

        """
        return self.init()._stores

    @property
    def data(self):
        """Get data associated with this job.

        This property should be used for large array-like data, which can't be
        stored efficiently in the job document. For examples and usage, see
        `Job Data Storage <https://docs.signac.io/en/latest/jobs.html#job-data-storage>`_.

        Equivalent to:

        .. code-block:: python

                return job.stores['signac_data']

        Returns
        -------
        :class:`~signac.H5Store`
            An HDF5-backed datastore.

        """
        return self.stores[self.KEY_DATA]

    @data.setter
    def data(self, new_data):
        """Assign new data to this job.

        Parameters
        ----------
        new_data : :class:`~signac.H5Store`
            An HDF5-backed datastore.

        """
        self.stores[self.KEY_DATA] = new_data

    def _init(self, force=False):
        """Contains all logic for job initialization.

        This method is called by :meth:`~.init` and is responsible
        for actually creating the job workspace directory and
        writing out the statepoint file.

        Parameters
        ----------
        force : bool
            If ``True``, write the job manifest even if it
            already exists. If ``False``, this method will
            raise an Exception if the manifest exists
            (Default value = False).

        """
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
            blob = json.dumps(self._statepoint, indent=2)

            try:
                # Open the file for writing only if it does not exist yet.
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
        """Check whether the manifest file is correct (if it exists)."""
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

        Parameters
        ----------
        force : bool
            Overwrite any existing state point's manifest
            files, e.g., to repair them if they got corrupted (Default value = False).

        Returns
        -------
        Job
            The job handle.

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
        initialized.

        """
        self.clear()
        self.init()

    def remove(self):
        """Remove the job's workspace including the job document.

        This function will do nothing if the workspace directory
        does not exist.

        """
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

        Parameters
        ----------
        project : :class:`~signac.Project`
            The project to move this job to.

        """
        dst = project.open_job(self.statepoint())
        _mkdir_p(project.workspace())
        try:
            os.replace(self.workspace(), dst.workspace())
        except OSError as error:
            if error.errno == errno.ENOENT:
                raise RuntimeError(
                    "Cannot move job '{}', because it is not initialized!".format(self))
            elif error.errno in (errno.EEXIST, errno.ENOTEMPTY, errno.EACCES):
                raise DestinationExistsError(dst)
            elif error.errno == errno.EXDEV:
                raise RuntimeError(
                    "Cannot move jobs across different devices (file systems).")
            else:
                raise error
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
        the raise of a :class:`~signac.errors.FileSyncConflict` exception.

        A document synchronization conflict can be resolved by providing a doc_sync function
        that takes the source and the destination document as first and second argument.

        Parameters
        ----------
        other : Job
            The other job to synchronize from.
        strategy :
            A synchronization strategy for file conflicts. If no strategy is provided, a
            :class:`~signac.errors.SyncConflict` exception will be raised upon conflict
            (Default value = None).
        exclude : str
            An filename exclude pattern. All files matching this pattern will be
            excluded from synchronization (Default value = None).
        doc_sync :
            A synchronization strategy for document keys. If this argument is None, by default
            no keys will be synchronized upon conflict.
        dry_run :
            If True, do not actually perform the synchronization.
        kwargs :
            Extra keyword arguments will be forward to the :meth:`~signac.sync.sync_jobs`
            function which actually excutes the synchronization operation.
        **kwargs :


        Raises
        ------
        FileSyncConflict
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

        Parameters
        ----------
        filename : str
            The name of the file.

        Returns
        -------
        str
            The full workspace path of the file.

        """
        return os.path.join(self._wd, filename)

    def isfile(self, filename):
        """Return True if file exists in the job's workspace.

        Parameters
        ----------
        filename : str
            The name of the file.

        Returns
        -------
        bool
            True if file with filename exists in workspace.

        """
        return os.path.isfile(self.fn(filename))

    def open(self):
        """Enter the job's workspace directory.

        You can use the `Job` class as context manager:

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
        """Close the job and switch to the previous working directory."""
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

    def __setstate__(self, state):
        self.__dict__.update(state)
        self._statepoint._parent.jobs.append(self)

    def __deepcopy__(self, memo):
        cls = self.__class__
        result = cls.__new__(cls)
        memo[id(self)] = result
        for k, v in self.__dict__.items():
            setattr(result, k, deepcopy(v, memo))
        return result
