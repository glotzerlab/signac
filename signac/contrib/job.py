# Copyright (c) 2017 The Regents of the University of Michigan.
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Job class defined here."""

import errno
import logging
import os
import shutil
from copy import deepcopy

from deprecation import deprecated

from ..core.h5store import H5StoreManager
from ..core.synced_collections.collection_json import JSONDict
from ..core.synced_collections.collection_json import (
    MemoryBufferedJSONDict as BufferedJSONDict,
)
from ..errors import KeyTypeError
from ..sync import sync_jobs
from ..version import __version__
from .errors import DestinationExistsError, JobsCorruptedError
from .hashing import calc_id
from .utility import _mkdir_p

logger = logging.getLogger(__name__)


class _LoadAndSaveSingleThread:
    """A context manager for :class:`SyncedCollection` to wrap saving and loading.

    Unclear how to mesh thread-safety with the fact that I'm introducing locks on a per-file level,
    but the save operation changes the filename within the context _and_ multiple jobs could
    point to the same statepoint. When the filename is changed by reset_statepoint, I need some
    way to consistently also repoint the locks.
    """

    def __init__(self, collection):
        self._collection = collection

    def __enter__(self):
        self._collection._load()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._collection._save()


class _StatepointDict(JSONDict):
    """A JSON-backed dictionary for storing job statepoints.

    There are two principal reasons for extending the base JSONDict:
        1. Saving needs to trigger a job directory migration, and
        2. Statepoints are assumed to not have to support external modification,
           so they never need to load from disk _except_ the very first time a
           job is opened by id and they're not present in the cache.
    """

    _PROTECTED_KEYS = ("_jobs",)
    _LoadSaveType = _LoadAndSaveSingleThread  # type: ignore

    def __init__(
        self,
        jobs=None,
        filename=None,
        write_concern=False,
        data=None,
        parent=None,
        *args,
        **kwargs,
    ):
        self._jobs = list(jobs)
        super().__init__(
            filename=filename,
            write_concern=write_concern,
            data=data,
            parent=parent,
            *args,
            **kwargs,
        )

    def _load(self):
        """Don't attempt a load unless no data was initially provided."""
        pass

    def _save(self):
        """Don't save to disk by default."""
        for job in self._jobs:
            job.reset_statepoint(self._data)

    def save(self, force):
        """Need a way to force a save to disk."""
        if force or not os.path.isfile(self._filename):
            super()._save()

    def load(self):
        """Need a way to force a save to disk."""
        super()._load()

    def move(self, new_filename):
        """Move to new filename."""
        os.replace(self.filename, new_filename)
        self._filename = new_filename


class Job:
    """The job instance is a handle to the data of a unique state point.

    Application developers should not directly instantiate this class, but
    use :meth:`~signac.Project.open_job` instead.

    Jobs can be opened by ``statepoint`` or ``_id``. If both values are
    provided, it is the user's responsibility to ensure that the values
    correspond.

    Parameters
    ----------
    project : :class:`~signac.Project`
        Project handle.
    statepoint : dict
        State point for the job. (Default value = None)
    _id : str
        The job identifier. (Default value = None)

    """

    FN_MANIFEST = "signac_statepoint.json"
    """The job's manifest filename.

    The job manifest is a human-readable file containing the job's state
    point that is stored in each job's workspace directory.
    """

    FN_DOCUMENT = "signac_job_document.json"
    "The job's document filename."

    KEY_DATA = "signac_data"
    "The job's datastore key."

    def __init__(self, project, statepoint=None, _id=None):
        self._project = project

        # Prepare wd in advance so that the attribute exists in checks below.
        self._wd = None

        if statepoint is None and _id is None:
            raise ValueError("Either statepoint or _id must be provided.")
        elif statepoint is not None:
            self._statepoint = _StatepointDict(jobs=[self], data=statepoint)
            try:
                self._id = calc_id(self._statepoint._to_base()) if _id is None else _id
            except TypeError:
                raise KeyTypeError
            self._statepoint._filename = self._statepoint_filename

            # Update the project's state point cache immediately if opened by state point
            self._project._register(self.id, statepoint)
        else:
            # Only an id was provided. State point will be loaded lazily.
            self._statepoint = None
            self._id = _id

        # Prepare job document
        self._document = None

        # Prepare job H5StoreManager
        self._stores = None

        # Prepare current working directory for context management
        self._cwd = []

    @deprecated(
        deprecated_in="1.3",
        removed_in="2.0",
        current_version=__version__,
        details="Use job.id instead.",
    )
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
        return hash(self.id)

    def __eq__(self, other):
        if not isinstance(other, type(self)):
            return NotImplemented
        return self.id == other.id and os.path.realpath(
            self.workspace()
        ) == os.path.realpath(other.workspace())

    def __str__(self):
        """Return the job's id."""
        return str(self.id)

    def __repr__(self):
        return "{}(project={}, statepoint={})".format(
            self.__class__.__name__, repr(self._project), self.statepoint
        )

    def workspace(self):
        """Return the job's unique workspace directory.

        See :ref:`signac job -w <signac-cli-job>` for the command line equivalent.

        Returns
        -------
        str
            The path to the job's workspace directory.

        """
        if self._wd is None:
            self._wd = os.path.join(self._project.workspace(), self.id)
        return self._wd

    @property
    def _statepoint_filename(self):
        """Get the path of the state point file for this job."""
        return os.path.join(self.workspace(), self.FN_MANIFEST)

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

        try:
            self.statepoint.move(self.statepoint.filename + "~")
            try:
                os.replace(self.workspace(), dst.workspace())
            except OSError as error:
                self._statepoint.move(self._statepoint.filename[:-1])  # rollback
                if error.errno in (errno.EEXIST, errno.ENOTEMPTY, errno.EACCES):
                    raise DestinationExistsError(dst)
                else:
                    raise
            else:
                dst.init()
        except OSError as error:
            if error.errno == errno.ENOENT:
                pass  # File is not initialized.
            else:
                raise

        # Update this instance
        self.statepoint._data = dst.statepoint._data
        self.statepoint._filename = dst.statepoint._filename
        self._id = dst._id
        self._wd = None
        self._document = None
        self._stores = None
        self._cwd = []
        logger.info(f"Moved '{self}' -> '{dst}'.")

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

        See :ref:`signac statepoint <signac-cli-statepoint>` for the command line equivalent.

        Returns
        -------
        dict
            Returns the job's state point.

        """
        if self._statepoint is None:
            # Load state point manifest lazily
            self._statepoint = _StatepointDict(
                jobs=[self], filename=self._statepoint_filename
            )

            try:
                self._statepoint.load()
                statepoint = self._statepoint()
            except ValueError:
                # This catches JSONDecodeError, a subclass of ValueError
                raise JobsCorruptedError([self.id])

            if calc_id(statepoint) != self.id:
                raise JobsCorruptedError([self.id])

            # Update the project's state point cache when loaded lazily
            self._project._register(self.id, statepoint)

        return self._statepoint

    @statepoint.setter
    def statepoint(self, new_statepoint):
        """Assign a new state point to this job.

        Parameters
        ----------
        new_statepoint : dict
            The new state point to be assigned.

        """
        self.reset_statepoint(new_statepoint)

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
            For more information, see
            :class:`~signac.core.synced_collections.collection_json.BufferedJSONDict`.

        See :ref:`signac document <signac-cli-document>` for the command line equivalent.

        Returns
        -------
        :class:`~signac.JSONDict`
            The job document handle.

        """
        # TODO: Fix this docstring explaining how to get a deep copy.
        if self._document is None:
            self.init()
            fn_doc = os.path.join(self.workspace(), self.FN_DOCUMENT)
            self._document = BufferedJSONDict(filename=fn_doc, write_concern=True)
        return self._document

    @document.setter
    def document(self, new_doc):
        """Assign new document to the this job.

        Parameters
        ----------
        new_doc : :class:`~signac.core.synced_collections.collection_json.BufferedJSONDict`
            The job document handle.

        """
        self.document.reset(new_doc)

    @property
    def doc(self):
        """Alias for :attr:`~Job.document`.

        .. warning::

            If you need a deep copy that will not modify the underlying
            persistent JSON file, use :attr:`~Job.document` instead of :attr:`~Job.doc`.
            For more information, see
            :class:`~signac.core.synced_collections.collection_json.BufferedJSONDict`.

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
        if self._stores is None:
            self.init()
            self._stores = H5StoreManager(self.workspace())
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

    def init(self, force=False):
        """Initialize the job's workspace directory.

        This function will do nothing if the directory and
        the job manifest already exist.

        Returns the calling job.

        See :ref:`signac job -c <signac-cli-job>` for the command line equivalent.

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
            # Attempt early exit if the manifest exists and is valid
            try:
                statepoint = self.statepoint._load_from_resource()
                if calc_id(statepoint) != self.id:
                    raise JobsCorruptedError([self.id])
            except Exception:
                # Any exception means this method cannot exit early.

                # Create the workspace directory if it did not exist yet.
                try:
                    _mkdir_p(self.workspace())
                except OSError:
                    logger.error(
                        "Error occurred while trying to create "
                        "workspace directory for job '{}'.".format(self)
                    )
                    raise

                try:
                    try:
                        # Open the file for writing only if it does not exist yet.
                        self.statepoint.save(force=force)
                    except OSError as error:
                        if error.errno not in (errno.EEXIST, errno.EACCES):
                            raise
                except Exception as error:
                    # Attempt to delete the file on error, to prevent corruption.
                    try:
                        os.remove(self._statepoint_filename)
                    except Exception:  # ignore all errors here
                        pass
                    raise error
                else:
                    try:
                        statepoint = self.statepoint._load_from_resource()
                    except ValueError:
                        # This catches JSONDecodeError, a subclass of ValueError
                        raise JobsCorruptedError([self.id])

                    if calc_id(statepoint) != self.id:
                        raise JobsCorruptedError([self.id])

            # Update the project's state point cache if the manifest is valid
            self._project._register(self.id, self.statepoint())
        except Exception:
            logger.error(
                f"State point manifest file of job '{self.id}' appears to be corrupted."
            )
            raise
        return self

    def clear(self):
        """Remove all job data, but not the job itself.

        This function will do nothing if the job was not previously
        initialized.

        See :ref:`signac rm -c <signac-cli-rm>` for the command line equivalent.

        """
        try:
            for fn in os.listdir(self.workspace()):
                if fn in (self.FN_MANIFEST, self.FN_DOCUMENT):
                    continue
                path = os.path.join(self.workspace(), fn)
                if os.path.isfile(path):
                    os.remove(path)
                elif os.path.isdir(path):
                    shutil.rmtree(path)
            self.document.clear()
        except OSError as error:
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

        See :ref:`signac rm <signac-cli-rm>` for the command line equivalent.

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
                except OSError as error:
                    if not error.errno == errno.ENOENT:
                        raise error
                self._document = None
            self._stores = None

    def move(self, project):
        """Move this job to project.

        This function will attempt to move this instance of job from
        its original project to a different project.

        See :ref:`signac move <signac-cli-move>` for the command line equivalent.

        Parameters
        ----------
        project : :class:`~signac.Project`
            The project to move this job to.

        """
        statepoint = self.statepoint()
        dst = project.open_job(statepoint)
        _mkdir_p(project.workspace())
        try:
            os.replace(self.workspace(), dst.workspace())
        except OSError as error:
            if error.errno == errno.ENOENT:
                raise RuntimeError(
                    f"Cannot move job '{self}', because it is not initialized!"
                )
            elif error.errno in (errno.EEXIST, errno.ENOTEMPTY, errno.EACCES):
                raise DestinationExistsError(dst)
            elif error.errno == errno.EXDEV:
                raise RuntimeError(
                    "Cannot move jobs across different devices (file systems)."
                )
            else:
                raise error
        self.__dict__.update(dst.__dict__)

        # Update the destination project's state point cache
        project._register(self.id, statepoint)

    def sync(self, other, strategy=None, exclude=None, doc_sync=None, **kwargs):
        r"""Perform a one-way synchronization of this job with the other job.

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
        \*\*kwargs :
            Extra keyword arguments will be forward to the :meth:`~signac.sync.sync_jobs`
            function which actually excutes the synchronization operation.

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
            **kwargs,
        )

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
        return os.path.join(self.workspace(), filename)

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
        logger.info(f"Enter workspace '{self.workspace()}'.")
        os.chdir(self.workspace())

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
        self.statepoint._jobs.append(self)

    def __deepcopy__(self, memo):
        cls = self.__class__
        result = cls.__new__(cls)
        memo[id(self)] = result
        for key, value in self.__dict__.items():
            setattr(result, key, deepcopy(value, memo))
        return result
