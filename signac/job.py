# Copyright (c) 2017 The Regents of the University of Michigan.
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Job class defined here."""

import errno
import hashlib
import json
import logging
import os
import shutil
from copy import deepcopy
from threading import RLock
from types import MappingProxyType
from typing import FrozenSet

from synced_collections.backends.collection_json import (
    BufferedJSONAttrDict,
    JSONAttrDict,
    json_attr_dict_validator,
)
from synced_collections.errors import KeyTypeError
from synced_collections.utils import SyncedCollectionJSONEncoder

from ._utility import _mkdir_p
from .errors import DestinationExistsError, JobsCorruptedError
from .h5store import H5StoreManager
from .sync import sync_jobs

logger = logging.getLogger(__name__)


def calc_id(statepoint):
    """Calculate and return a hash value for the given statepoint.

    The hash is computed as an MD5 checksum of the input data. The input data
    is first encoded as JSON, with dictionary keys sorted to ensure the hash
    is reproducible.

    Parameters
    ----------
    statepoint : dict
        A JSON-encodable mapping.

    Returns
    -------
    str
        Encoded hash in hexadecimal format.
    """
    blob = json.dumps(statepoint, cls=SyncedCollectionJSONEncoder, sort_keys=True)
    m = hashlib.md5()
    m.update(blob.encode())
    return m.hexdigest()


# Note: All children of _StatePointDict will be of its parent type because they
# share a backend and the SyncedCollection registry parses the classes in order
# of registration. _If_ we need more control over this, that process can be
# exposed more thoroughly and registration can be made explicit rather than
# implicit, but for now the existing behavior works fine.
class _StatePointDict(JSONAttrDict):
    """A JSON-backed dictionary for storing job state points.

    There are three principal reasons for extending the base JSONAttrDict:
        1. Saving needs to trigger a job directory migration, and
        2. State points are assumed to not support external modification, so
           they never need to load from disk _except_ the very first time a job
           is opened by id and the state point is not present in the cache.
        3. It must be possible to load and/or save on demand during tasks like
           job directory migrations.
    """

    _PROTECTED_KEYS: FrozenSet[str] = JSONAttrDict._PROTECTED_KEYS.union(("_jobs",))
    _all_validators = (json_attr_dict_validator,)

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
        # Multiple Python Job objects can share a single `_StatePointDict`
        # instance because they are shallow copies referring to the same data
        # on disk. We need to store these jobs in a shared list here so that
        # shallow copies can point to the same place and trigger each other to
        # update. This does not apply to independently created Job objects,
        # even if they refer to the same disk data; this only applies to
        # explicit shallow copies and unpickled objects within a session.
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
        # State points never load from disk automatically. They are either
        # initialized with provided data (e.g. from the state point cache), or
        # they load from disk the first time state point data is requested for
        # a Job opened by id (in which case the state point must first be
        # validated manually).
        pass

    def _save(self):
        # State point modification triggers job migration for all jobs sharing
        # this state point (shallow copies of a single job).
        new_id = calc_id(self)

        # All elements of the job list are shallow copies of each other, so any
        # one of them is representative.
        job = next(iter(self._jobs))
        old_id = job._id
        if old_id == new_id:
            return

        tmp_statepoint_file = self.filename + "~"
        should_init = False
        try:
            # Move the state point to an intermediate location as a backup.
            os.replace(self.filename, tmp_statepoint_file)
            try:
                new_workspace = os.path.join(job._project.workspace, new_id)
                os.replace(job.path, new_workspace)
            except OSError as error:
                os.replace(tmp_statepoint_file, self.filename)  # rollback
                if error.errno in (errno.EEXIST, errno.ENOTEMPTY, errno.EACCES):
                    raise DestinationExistsError(new_id)
                else:
                    raise
            else:
                should_init = True
        except OSError as error:
            # The most likely reason we got here is because the state point
            # file move failed due to the job not being initialized so the file
            # doesn't exist, which is OK.
            if error.errno != errno.ENOENT:
                raise

        # Update each job instance.
        for job in self._jobs:
            job._id = new_id
            job._initialize_lazy_properties()

        # Remove the temporary state point file if it was created. Have to do it
        # here so that we can get the updated job state point filename and set
        # it to self.filename below.
        try:
            os.remove(job._statepoint_filename + "~")
        except OSError as error:
            if error.errno != errno.ENOENT:
                raise

        self.filename = job._statepoint_filename

        if should_init:
            # Only initializing one job assumes that all changes in init are
            # changes reflected in the underlying resource (the JSON file).
            # This assumption is currently valid because all in-memory
            # attributes are loaded lazily (and are handled by the call to
            # _initialize_lazy_properties above), except for the key defining
            # property of the job id (which is also updated above). If init
            # ever changes to making modifications to the job object, we may
            # need to call it for all jobs.
            job.init()

        logger.info(f"Moved '{old_id}' -> '{new_id}'.")

    def save(self, force=False):
        """Trigger a save to disk.

        Unlike normal JSONAttrDict objects, this class requires the ability to save
        on command. Moreover, this save must be conditional on whether or not a
        file is present to allow the user to observe state points in corrupted
        data spaces and attempt to recover.

        Parameters
        ----------
        force : bool
            If True, save even if the file is present on disk.
        """
        try:
            # Open the file for writing only if it does not exist yet.
            if force or not os.path.isfile(self.filename):
                super()._save()
        except Exception as error:
            if not isinstance(error, OSError) or error.errno not in (
                errno.EEXIST,
                errno.EACCES,
            ):
                # Attempt to delete the file on error, to prevent corruption.
                # OSErrors that are EEXIST or EACCES don't need to delete the file.
                try:
                    os.remove(self.filename)
                except Exception:  # ignore all errors here
                    pass
                raise

    def load(self, job_id):
        """Trigger a load from disk.

        Unlike normal JSONAttrDict objects, this class requires the ability to
        load on command. These loads typically occur when the state point
        must be validated against the data on disk; at all other times, the
        in-memory data is assumed to be accurate to avoid unnecessary I/O.

        Parameters
        ----------
        job_id : str
            Job id used to validate contents on disk.

        Returns
        -------
        data : dict
            Dictionary of state point data.

        Raises
        ------
        :class:`~signac.errors.JobsCorruptedError`
            If the data on disk is invalid or its hash does not match the job
            id.

        """
        try:
            data = self._load_from_resource()
        except json.JSONDecodeError:
            raise JobsCorruptedError([job_id])

        if calc_id(data) != job_id:
            raise JobsCorruptedError([job_id])

        with self._suspend_sync:
            self._update(data, _validate=False)

        return data


class Job:
    """The job instance is a handle to the data of a unique state point.

    Application developers should not directly instantiate this class, but
    use :meth:`~signac.Project.open_job` instead.

    Jobs can be opened by ``statepoint`` or ``id_``. If both values are
    provided, it is the user's responsibility to ensure that the values
    correspond. Set ``directory_known`` to ``True`` when the job directory
    is known to exist - this skips some expensive isdir checks.

    Parameters
    ----------
    project : :class:`~signac.Project`
        Project handle.
    statepoint : dict, optional
        State point for the job. (Default value = None)
    id_ : str, optional
        The job identifier. (Default value = None)
    directory_known : bool, optional
        Set to true when the job directory is known to exist. (Default value = False)

    """

    FN_STATE_POINT = "signac_statepoint.json"
    """The job's state point filename.

    The job state point is a human-readable file containing the job's state
    point that is stored in each job's workspace directory.
    """

    FN_DOCUMENT = "signac_job_document.json"
    "The job's document filename."

    KEY_DATA = "signac_data"
    "The job's datastore key."

    def __init__(self, project, statepoint=None, id_=None, directory_known=False):
        self._project = project
        self._lock = RLock()
        self._initialize_lazy_properties()
        self._directory_known = directory_known

        if statepoint is None and id_ is None:
            raise ValueError("Either statepoint or id_ must be provided.")
        elif statepoint is not None:
            try:
                self._id = calc_id(statepoint) if id_ is None else id_
            except TypeError:
                raise KeyTypeError

            self._cached_statepoint = statepoint
            self._statepoint_requires_init = True
        else:
            # Only an id was provided. State point will be loaded lazily.
            self._id = id_
            self._statepoint_requires_init = True

            # Fetch the cached statepoint from the project's cache. Don't load it
            # from disk on a cache miss (will be loaded on demand).
            try:
                self._cached_statepoint = project._sp_cache[id_]
            except KeyError:
                self._cached_statepoint = None

    def _initialize_lazy_properties(self):
        """Initialize all properties that are designed to be loaded lazily."""
        with self._lock:
            self._path = None
            self._document = None
            self._stores = None
            self._cwd = []

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
        return self.id == other.id and os.path.realpath(self.path) == os.path.realpath(
            other.path
        )

    def __str__(self):
        """Return the job's id."""
        return str(self.id)

    def __repr__(self):
        return "{}(project={}, statepoint={})".format(
            self.__class__.__name__, repr(self._project), self.cached_statepoint
        )

    @property
    def _statepoint_filename(self):
        """Get the path of the state point file for this job."""
        # Performance-critical path. We can rely on the job workspace and state
        # point file name to be well-formed, so just use str.join with os.sep
        # instead of os.path.join for speed.
        return os.sep.join((self.path, self.FN_STATE_POINT))

    @property
    def path(self):
        """str: The path to the job directory.

        See :ref:`signac job -w <signac-cli-job>` for the command line equivalent.
        """
        if self._path is None:
            # We can rely on the project workspace to be well-formed, so just
            # use str.join with os.sep instead of os.path.join for speed.
            self._path = os.sep.join((self._project.workspace, self.id))
        return self._path

    def update_statepoint(self, update, overwrite=False):
        """Change the state point of this job while preserving job data.

        By default, this method will not change existing parameters of the
        state point of the job.

        This method will change the job id if the state point has been altered.

        For more information, see
        `Modifying the State Point <https://docs.signac.io/en/latest/jobs.html#modifying-the-state-point>`_.

        .. warning::

            While appending to a job's state point is generally safe, modifying
            existing parameters may lead to data inconsistency. Use the
            ``overwrite`` argument with caution!

        Parameters
        ----------
        update : dict
            A mapping used for the state point update.
        overwrite : bool, optional
            If False, an error will be raised if the update modifies the values
            of existing keys in the state point. If True, any existing keys will
            be overwritten in the same way as :meth:`dict.update`. Use with
            caution! (Default value = False).

        Raises
        ------
        KeyError
            If the update contains keys which are already part of the job's
            state point and ``overwrite`` is False.
        :class:`~signac.errors.DestinationExistsError`
            If a job associated with the new state point is already initialized.
        OSError
            If the move failed due to an unknown system related error.

        """  # noqa: E501
        statepoint = self.statepoint()
        if not overwrite:
            for key, value in update.items():
                if statepoint.get(key, value) != value:
                    raise KeyError(
                        f"Key {key} was provided but already exists in the "
                        "mapping with another value."
                    )
        statepoint.update(update)
        self.statepoint = statepoint

    @property
    def cached_statepoint(self):
        """Get a copy of the job's state point as a read-only mapping.

        :py:attr:`cached_statepoint` uses the state point cache to provide fast access to
        the job's state point for reading.

        .. note::

            Create and update the state point cache by calling
            :py:meth:`project.update_cache <signac.Project.update_cache>`
            or running ``signac update-cache`` on the command line.

        .. seealso::

            Use :py:attr:`statepoint` to modify the job's state point.

        Returns
        -------
        Mapping
            Returns the job's state point.
        """
        if self._cached_statepoint is None:
            self._cached_statepoint = self._project._get_statepoint(self._id)

        return MappingProxyType(self._cached_statepoint)

    @property
    def statepoint(self):
        """Get or set the job's state point.

        Setting the state point to a different value will change the job id.

        For more information, see
        `Modifying the State Point
        <https://docs.signac.io/en/latest/jobs.html#modifying-the-state-point>`_.

        .. tip::

            Use :py:attr:`cached_statepoint` for fast read-only access to the
            state point.

        .. warning::

            The state point object behaves like a dictionary in most cases,
            but because it persists changes to the filesystem, making a copy
            requires explicitly converting it to a dict. If you need a
            modifiable copy that will not modify the underlying JSON file,
            you can access a dict copy of the state point by calling it, e.g.
            ``sp_dict = job.statepoint()`` instead of ``sp = job.statepoint``.
            For more information, see
            :class:`~synced_collections.backends.collection_json.JSONAttrDict`.

        See :ref:`signac statepoint <signac-cli-statepoint>` for the command line equivalent.

        .. danger::

            Use this function with caution! Resetting a job's state point
            may sometimes be necessary, but can possibly lead to incoherent
            data spaces.

        Returns
        -------
        MutableMapping
            Returns the job's state point. Supports attribute-based access to
            dict keys.
        """
        with self._lock:
            if self._statepoint_requires_init:
                if self._cached_statepoint is None:
                    # Load state point data lazily (on access).
                    self._statepoint = _StatePointDict(
                        jobs=[self],
                        filename=self._statepoint_filename,
                    )
                    statepoint = self._statepoint.load(self.id)

                    # Update the project's state point cache when loaded lazily
                    self._project._register(self.id, statepoint)
                    self._cached_statepoint = statepoint
                else:
                    # Create _StatePointDict lazily with a known statepoint dict.
                    self._statepoint = _StatePointDict(
                        jobs=[self],
                        filename=self._statepoint_filename,
                        data=self._cached_statepoint,
                    )

                self._statepoint_requires_init = False

        return self._statepoint

    @statepoint.setter
    def statepoint(self, new_statepoint):
        """Assign a new state point to this job.

        Parameters
        ----------
        new_statepoint : dict
            The new state point to be assigned.
        """
        with self._lock:
            if self._statepoint_requires_init:
                # Instantiate state point data lazily - no load is required, since
                # we are provided with the new state point data.
                self._statepoint = _StatePointDict(
                    jobs=[self], filename=self._statepoint_filename
                )
                self._statepoint_requires_init = False
            self.statepoint.reset(new_statepoint)

        self._project._register(self.id, new_statepoint)

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

            Even deep copies of :attr:`~Job.document` will modify the same file,
            so changes will still effectively be persisted between deep copies.
            If you need a deep copy that will not modify the underlying
            persistent JSON file, use the call operator to get an equivalent
            plain dictionary: ``job.document()``.
            For more information, see
            :class:`~signac.JSONDict`.

        See :ref:`signac document <signac-cli-document>` for the command line equivalent.

        Returns
        -------
        MutableMapping
            The job document handle. Supports attribute-based access to dict keys.

        """
        with self._lock:
            if self._document is None:
                self.init(validate_statepoint=False)
                fn_doc = os.path.join(self.path, self.FN_DOCUMENT)
                self._document = BufferedJSONAttrDict(
                    filename=fn_doc, write_concern=True
                )
        return self._document

    @document.setter
    def document(self, new_doc):
        """Assign new document data to this job.

        Parameters
        ----------
        new_doc : dict
            The job document handle.

        """
        self.document.reset(new_doc)

    @property
    def doc(self):
        """Alias for :attr:`~Job.document`.

        .. warning::

            Even deep copies of :attr:`~Job.doc` will modify the same file, so
            changes will still effectively be persisted between deep copies.
            If you need a deep copy that will not modify the underlying
            persistent JSON file, use the call operator to get an equivalent
            plain dictionary: ``job.doc()``.

        See :ref:`signac document <signac-cli-document>` for the command line equivalent.

        Returns
        -------
        MutableMapping
            The job document handle. Supports attribute-based access to dict keys.

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
        with self._lock:
            if self._stores is None:
                self.init(validate_statepoint=False)
                self._stores = H5StoreManager(self.path)
        return self._stores

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

    @property
    def project(self):
        """Get the project that contains this job.

        Returns
        -------
        signac.Project
            Returns the project containing this job.
        """
        return self._project

    def init(self, force=False, validate_statepoint=True):
        """Initialize the job's workspace directory.

        This function will do nothing if the directory and the job state point
        already exist and the state point is valid.

        Returns the calling job.

        See :ref:`signac job -c <signac-cli-job>` for the command line equivalent.

        Parameters
        ----------
        force : bool, optional
            Overwrite any existing state point files, e.g., to repair them if
            they got corrupted (Default value = False).

        validate_statepoint : bool, optional
            When True (the default), load the job state point and ensure that it matches
            the id. When False, exit early when the job directory exists.

        Returns
        -------
        Job
            The job handle.

        Raises
        ------
        OSError
            If the workspace directory cannot be created or any other I/O error
            occurs when attempting to save the state point file.
        JobsCorruptedError
            If the job state point on disk is corrupted.
        """
        with self._lock:
            try:
                # Fast early exit when not validating.
                if not validate_statepoint:
                    if self._directory_known:
                        return self

                    if os.path.isdir(self.path):
                        self._directory_known = True
                        return self

                # Attempt early exit if the state point file exists and is valid.
                try:
                    statepoint = self.statepoint.load(self.id)
                except Exception:
                    # Any exception means this method cannot exit early.

                    # Create the workspace directory if it does not exist.
                    try:
                        _mkdir_p(self.path)
                    except OSError:
                        logger.error(
                            "Error occurred while trying to create "
                            "workspace directory for job '{}'.".format(self.id)
                        )
                        raise

                    self._directory_known = True

                    # The state point save will not overwrite an existing file on
                    # disk unless force is True, so the subsequent load will catch
                    # when a preexisting invalid file was present.
                    self.statepoint.save(force=force)
                    statepoint = self.statepoint.load(self.id)

                    # Update the project's state point cache if the saved file is valid.
                    self._project._register(self.id, statepoint)
            except Exception:
                logger.error(
                    f"State point file of job '{self.id}' appears to be corrupted."
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
            for fn in os.listdir(self.path):
                if fn in (self.FN_STATE_POINT, self.FN_DOCUMENT):
                    continue
                path = os.path.join(self.path, fn)
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
        with self._lock:
            try:
                shutil.rmtree(self.path)
            except OSError as error:
                if error.errno != errno.ENOENT:
                    raise
            else:
                if self._document is not None:
                    try:
                        self._document.clear()
                    except OSError as error:
                        if error.errno != errno.ENOENT:
                            raise error
                    self._document = None
                self._stores = None

            self._directory_known = False

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
        with self._lock:
            statepoint = self.statepoint()
            dst = project.open_job(statepoint)
            _mkdir_p(project.workspace)
            try:
                os.replace(self.path, dst.path)
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
        strategy : callable, optional
            A synchronization strategy for file conflicts. If no strategy is provided, a
            :class:`~signac.errors.SyncConflict` exception will be raised upon conflict
            (Default value = None).
        exclude : str, optional
            A filename exclude pattern. All files matching this pattern will be
            excluded from synchronization (Default value = None).
        doc_sync : attribute or callable from :py:class:`~signac.sync.DocSync`, optional
            A synchronization strategy for document keys. If this argument is None, by default
            no keys will be synchronized upon conflict (Default value = None).
        dry_run : bool, optional
            If True, do not actually perform the synchronization.
        \*\*kwargs :
            Extra keyword arguments will be forwarded to the :meth:`~signac.sync.sync_jobs`
            function which actually executes the synchronization operation.

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
        """Prepend a filename with the job path.

        Parameters
        ----------
        filename : str
            The name of the file.

        Returns
        -------
        str
            The absolute path to the file.

        """
        return os.path.join(self.path, filename)

    def isfile(self, filename):
        """Check if a filename exists in the job directory.

        Parameters
        ----------
        filename : str
            The name of the file.

        Returns
        -------
        bool
            True if filename exists in the job directory.

        """
        return os.path.isfile(self.fn(filename))

    def open(self):
        """Enter the job's workspace directory.

        You can use the `Job` class as context manager:

        .. code-block:: python

            with project.open_job(my_statepoint) as job:
                # Manipulate your job data
                pass

        Opening the context will switch into the job's workspace,
        leaving it will switch back to the previous working directory.

        """
        self._cwd.append(os.getcwd())
        self.init(validate_statepoint=False)
        logger.info(f"Enter workspace '{self.path}'.")
        os.chdir(self.path)

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

    def __getstate__(self):
        state = dict(self.__dict__)
        # Locks are not pickleable and must be removed from the state
        del state["_lock"]
        return state

    def __setstate__(self, state):
        # Locks are not pickleable and must be added back to the state
        state["_lock"] = RLock()
        self.__dict__.update(state)
        # We append to a list of jobs rather than replacing to support
        # transparent id updates between shallow copies of a job.
        self.statepoint._jobs.append(self)

    def __deepcopy__(self, memo):
        cls = self.__class__
        result = cls.__new__(cls)
        memo[id(self)] = result
        state = dict(self.__dict__)
        # Locks are not pickleable and must be removed/added back
        del state["_lock"]
        for key, value in state.items():
            setattr(result, key, deepcopy(value, memo))
        result._lock = RLock()
        return result
