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

from ..core import json
from ..core.attrdict import SyncedAttrDict
from ..core.h5store import H5StoreManager
from ..core.jsondict import JSONDict
from ..sync import sync_jobs
from ..version import __version__
from .errors import DestinationExistsError, JobsCorruptedError
from .hashing import calc_id
from .utility import _mkdir_p

logger = logging.getLogger(__name__)


class _sp_save_hook:
    """Hook to handle job migration when state points are changed.

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

        if statepoint is None and _id is None:
            raise ValueError("Either statepoint or _id must be provided.")
        elif statepoint is not None:
            # A state point was provided.
            self._statepoint = SyncedAttrDict(statepoint, parent=_sp_save_hook(self))
            # If the id is provided, assume the job is already registered in
            # the project cache and that the id is valid for the state point.
            if _id is None:
                # Validate the state point and recursively convert to supported types.
                statepoint = self.statepoint()
                # Compute the id from the state point if not provided.
                self._id = calc_id(statepoint)
                # Update the project's state point cache immediately if opened by state point
                self._project._register(self.id, statepoint)
            else:
                self._id = _id
        else:
            # Only an id was provided. State point will be loaded lazily.
            self._statepoint = None
            self._id = _id

        # Prepare job working directory
        self._wd = None

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
    def ws(self):
        """Alias for :meth:`~Job.workspace`."""
        return self.workspace()

    def reset_statepoint(self, new_statepoint):
        """Overwrite the state point of this job while preserving job data.

        This method will change the job id if the state point has been altered.

        For more information, see
        `Modifying the State Point <https://docs.signac.io/en/latest/jobs.html#modifying-the-state-point>`_.

        .. danger::

            Use this function with caution! Resetting a job's state point
            may sometimes be necessary, but can possibly lead to incoherent
            data spaces.

        Parameters
        ----------
        new_statepoint : dict
            The job's new state point.

        """  # noqa: E501
        dst = self._project.open_job(new_statepoint)
        if dst == self:
            return
        fn_manifest = os.path.join(self.workspace(), self.FN_MANIFEST)
        fn_manifest_backup = fn_manifest + "~"
        try:
            os.replace(fn_manifest, fn_manifest_backup)
            try:
                os.replace(self.workspace(), dst.workspace())
            except OSError as error:
                os.replace(fn_manifest_backup, fn_manifest)  # rollback
                if error.errno in (errno.EEXIST, errno.ENOTEMPTY, errno.EACCES):
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
        self.statepoint._data = dst.statepoint._data
        self._id = dst._id
        self._wd = None
        self._document = None
        self._stores = None
        self._cwd = []
        logger.info(f"Moved '{self}' -> '{dst}'.")

    def _reset_sp(self, new_statepoint=None):
        """Check for new state point requested to assign this job.

        Parameters
        ----------
        new_statepoint : dict
            The job's new state point (Default value = None).

        """
        if new_statepoint is None:
            new_statepoint = self.statepoint()
        self.reset_statepoint(new_statepoint)

    def update_statepoint(self, update, overwrite=False):
        """Change the state point of this job while preserving job data.

        By default, this method will not change existing parameters of the
        state point of the job.

        This method will change the job id if the state point has been altered.

        For more information, see
        `Modifying the State Point <https://docs.signac.io/en/latest/jobs.html#modifying-the-state-point>`_.

        .. warning::

            While appending to a job's state point is generally safe,
            modifying existing parameters may lead to data
            inconsistency. Use the overwrite argument with caution!

        Parameters
        ----------
        update : dict
            A mapping used for the state point update.
        overwrite : bool, optional
            If True, this method will set all existing and new parameters
            to a job's statepoint, making it equivalent to
            :meth:`~.reset_statepoint`. Use with caution!
            (Default value = False).

        Raises
        ------
        KeyError
            If the update contains keys, which are already part of the job's
            state point and overwrite is False.
        :class:`~signac.errors.DestinationExistsError`
            If a job associated with the new state point is already initialized.
        OSError
            If the move failed due to an unknown system related error.

        """  # noqa: E501
        statepoint = self.statepoint()
        if not overwrite:
            for key, value in update.items():
                if statepoint.get(key, value) != value:
                    raise KeyError(key)
        statepoint.update(update)
        self.reset_statepoint(statepoint)

    def _read_manifest(self):
        """Read and parse the manifest file, if it exists.

        Returns
        -------
        manifest : dict
            State point data.

        Raises
        ------
        :class:`~signac.errors.JobsCorruptedError`
            If an error occurs while parsing the state point manifest.
        OSError
            If an error occurs while reading the state point manifest.

        """
        fn_manifest = os.path.join(self.workspace(), self.FN_MANIFEST)
        try:
            with open(fn_manifest, "rb") as file:
                manifest = json.loads(file.read().decode())
        except OSError as error:
            if error.errno != errno.ENOENT:
                raise error
        except ValueError:
            # This catches JSONDecodeError, a subclass of ValueError
            raise JobsCorruptedError([self.id])
        else:
            return manifest

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
            # Load state point manifest lazily and assign to
            # self._statepoint
            statepoint = self._check_manifest()
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
        self._reset_sp(new_statepoint)

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

        See :ref:`signac document <signac-cli-document>` for the command line equivalent.

        Returns
        -------
        :class:`~signac.JSONDict`
            The job document handle.

        """
        if self._document is None:
            self.init()
            fn_doc = os.path.join(self.workspace(), self.FN_DOCUMENT)
            self._document = JSONDict(filename=fn_doc, write_concern=True)
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

    def _init(self, force=False):
        """Contains all logic for job initialization.

        This method is called by :meth:`~.init` and is responsible
        for actually creating the job workspace directory and
        writing out the state point manifest file.

        Parameters
        ----------
        force : bool
            If ``True``, write the job manifest even if it
            already exists. If ``False``, this method will
            raise an Exception if the manifest exists
            (Default value = False).

        """
        # Attempt early exit if the manifest exists and is valid
        try:
            statepoint = self._check_manifest()
        except Exception:
            # Any exception means this method cannot exit early.

            # Create the workspace directory if it does not exist.
            try:
                _mkdir_p(self.workspace())
            except OSError:
                logger.error(
                    "Error occurred while trying to create "
                    "workspace directory for job '{}'.".format(self.id)
                )
                raise

            fn_manifest = os.path.join(self.workspace(), self.FN_MANIFEST)
            try:
                # Prepare the data before file creation and writing.
                statepoint = self.statepoint()
                blob = json.dumps(statepoint, indent=2)
            except JobsCorruptedError:
                raise

            try:
                # Open the file for writing only if it does not exist yet.
                with open(fn_manifest, "w" if force else "x") as file:
                    file.write(blob)
            except OSError as error:
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
                # Validate the output again after writing to disk
                statepoint = self._check_manifest()

        # Update the project's state point cache if the manifest is valid
        self._project._register(self.id, statepoint)

    def _check_manifest(self):
        """Check whether the manifest file exists and is correct.

        If the manifest is valid, this sets the state point if it is not
        already set.

        Returns
        -------
        manifest : dict
            State point data.

        Raises
        ------
        :class:`~signac.errors.JobsCorruptedError`
            If the manifest hash is not equal to the job id.

        """
        manifest = self._read_manifest()
        if calc_id(manifest) != self.id:
            raise JobsCorruptedError([self.id])
        if self._statepoint is None:
            self._statepoint = SyncedAttrDict(manifest, parent=_sp_save_hook(self))
        return manifest

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
            self._init(force=force)
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
        :class:`~signac.errors.FileSyncConflict`
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
        self.statepoint._parent.jobs.append(self)

    def __deepcopy__(self, memo):
        cls = self.__class__
        result = cls.__new__(cls)
        memo[id(self)] = result
        for key, value in self.__dict__.items():
            setattr(result, key, deepcopy(value, memo))
        return result
