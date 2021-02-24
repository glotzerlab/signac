# Copyright (c) 2020 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""A standardized buffering implementation for file-based backends.

The buffering method implemented here involves a single buffer of references to
in-memory objects containing data. These objects are the base types of a given
:class:`~.SyncedCollection` type, e.g. a dict for all dict-like collections,
and are the underlying data stores for those types. This buffering method
exploits the fact that all mutable collection types in Python are references,
so modifying one such collection results in modifying all of them, thereby
removing any need for more complicated synchronization protocols.
"""

from ..errors import MetadataError
from .file_buffered_collection import FileBufferedCollection


class SharedMemoryFileBufferedCollection(FileBufferedCollection):
    """A :class:`~.SyncedCollection` that defers all I/O when buffered.

    This class extends the :class:`~.FileBufferedCollection` and implements a
    concrete storage mechanism in which collections store a reference to their
    data in a buffer. This method takes advantage of the reference-based semantics
    of built-in Python mutable data types like dicts and lists. All collections
    referencing the same file are pointed to the same underlying data store in
    buffered mode, allowing all changes in one to be transparently reflected in
    the others. To further improve performance, the buffer size is determined
    only based on the number of modified collections stored, not the total number.
    As a result, the maximum capacity is only reached when a large number of
    modified collections are stored, and unmodified collections are only removed
    from the buffer when a buffered context is exited (rather than when buffer
    capacity is exhausted). See the Warnings section for more information.

    The buffer size and capacity for this class is measured in the total number
    of collections stored in the buffer that have undergone any modifications
    since their initial load from disk. A sequence of read-only operations will
    load data into the buffer, but the apparent buffer size will be zero.

    .. note::
        Important note for subclasses: This class should be inherited before
        any other collections. This requirement is due to the extensive use of
        multiple inheritance: since this class is designed to be combined with
        other :class:`~.SyncedCollection` types without making those types aware
        of buffering behavior, it transparently hooks into the initialization
        process, but this is dependent on its constructor being called before
        those of other classes.

    **Thread safety**

    This buffering method is thread safe. This thread safety is independent of the
    safety of an individual collection backend; the backend must support thread
    safe writes to the underlying resource in order for a buffered version using
    this class to be thread safe for general use. The thread safety guaranteed
    by this class only concerns buffer reads, writes, and flushes. All these
    operations are serialized because there is no way to prevent one collection
    from triggering a flush while another still thinks its data is in the cache;
    however, this shouldn't be terribly performance-limiting since in buffered
    mode we're avoiding I/O anyway and that's the only thing that can be effectively
    parallelized here.

    Parameters
    ----------
    filename : str, optional
        The filename of the associated JSON file on disk (Default value = None).

    Warnings
    --------
    - Although it can be done safely, in general modifying two different collections
      pointing to the same underlying resource while both are in different buffering
      modes is unsupported and can lead to undefined behavior. This class makes a
      best effort at performing safe modifications, but it is possible to construct
      nested buffered contexts for different objects that can lead to an invalid
      buffer state, or even situations where there is no obvious indicator of what
      is the canonical source of truth. In general, if you need multiple objects
      pointing to the same resource, it is **strongly** recommeneded to work with
      both of them in identical buffering states at all times.
    - This buffering method has no upper bound on the buffer size if all
      operations on buffered objects are read-only operations. If a strict upper bound
      is required, for instance due to strict virtual memory limits on a given system,
      use of the :class:`~.SerializedFileBufferedCollection` will allow limiting
      the total memory usage of the process.

    """

    _BUFFER_CAPACITY = 1000  # The number of collections to store in the buffer.

    def _flush(self, force=False):
        """Save buffered changes to the underlying file.

        Parameters
        ----------
        force : bool
            If True, force a flush even in buffered mode (defaults to False). This
            parameter is used when the buffer is filled to capacity.

        Raises
        ------
        MetadataError
            If any file is detected to have changed on disk since it was
            originally loaded into the buffer and modified.

        """
        # Different files in the buffer can be safely flushed simultaneously,
        # but a given file can only be flushed on one thread at once.
        if not self._is_buffered or force:
            try:
                cached_data = type(self)._buffer[self._filename]
            except KeyError:
                # If we got to this point, it means that another collection
                # pointing to the same underlying resource flushed the buffer.
                # If so, then the data in this instance is still pointing to
                # that object's data store. If this was a force flush, then
                # the data store is still the cached data, so we're fine. If
                # this wasn't a force flush, then we have to reload this
                # object's data so that it will stop sharing data with the
                # other instance.
                if not force:
                    self._data.clear()
                    self._update(self._load_from_resource())
            else:
                # If the contents have not been changed since the initial read,
                # we don't need to rewrite it.
                try:
                    # Validate that the file hasn't been changed by
                    # something else.
                    if cached_data["modified"]:
                        if cached_data["metadata"] != self._get_file_metadata():
                            raise MetadataError(self._filename, cached_data["contents"])
                        self._save_to_resource()
                finally:
                    # Whether or not an error was raised, the cache must be
                    # cleared to ensure a valid final buffer state, unless
                    # we're force flushing in which case we never delete, but
                    # take note that the data is no longer modified relative to
                    # its representation on disk.
                    if cached_data["modified"]:
                        type(self)._CURRENT_BUFFER_SIZE -= 1
                    if not force:
                        del type(self)._buffer[self._filename]
                    else:
                        # Have to update the metadata on a force flush because
                        # we could modify this item again later, leading to
                        # another (possibly forced) flush afterwards that will
                        # appear invalid if the metadata isn't updated to the
                        # metadata after the current flush.
                        cached_data["metadata"] = self._get_file_metadata()
                        cached_data["modified"] = False
        else:
            # If this object is still buffered _and_ this wasn't a force flush,
            # that implies a nesting of buffered contexts in which another
            # collection pointing to the same data flushed the buffer. This
            # object's data will still be pointing to that one, though, so the
            # safest choice is to reinitialize its data from scratch.
            with self._suspend_sync:
                self._data = {
                    key: self._from_base(data=value, parent=self)
                    for key, value in self._to_base().items()
                }

    def _load(self):
        """Load data from the backend but buffer if needed.

        Override the base buffered method to skip the _update and to let
        _load_from_buffer happen "in place."
        """
        if not self._suspend_sync:
            if self._root is None:
                if self._is_buffered:
                    self._load_from_buffer()
                else:
                    data = self._load_from_resource()
                    with self._suspend_sync:
                        self._update(data)
            else:
                self._root._load()

    def _save_to_buffer(self):
        """Store data in buffer.

        See :meth:`~._initialize_data_in_buffer` for details on the data stored
        in the buffer and the integrity checks performed.
        """
        type(self)._buffered_collections[id(self)] = self

        # Since one object could write to the buffer and trigger a flush while
        # another object was found in the buffer and attempts to proceed
        # normally, we have to serialize this whole block. In theory we might
        # be safe without it because the only operations that should reach this
        # point without already being locked are destructive operations (clear,
        # reset) that don't use the :meth:`_load_and_save` context, and for
        # those the writes will be automatically serialized because Python
        # dicts are thread-safe because of the GIL. However, it's best not to
        # depend on the thread-safety of built-in containers.
        with self._buffer_lock:
            if self._filename in type(self)._buffer:
                # Always track all instances pointing to the same data.

                # If all we had to do is set the flag, it could be done without any
                # check, but we also need to increment the number of modified
                # items, so we may as well do the update conditionally as well.
                if not type(self)._buffer[self._filename]["modified"]:
                    type(self)._buffer[self._filename]["modified"] = True
                    type(self)._CURRENT_BUFFER_SIZE += 1
            else:
                self._initialize_data_in_buffer(modified=True)
                type(self)._CURRENT_BUFFER_SIZE += 1

            if type(self)._CURRENT_BUFFER_SIZE > type(self)._BUFFER_CAPACITY:
                type(self)._flush_buffer(force=True)

    def _load_from_buffer(self):
        """Read data from buffer.

        See :meth:`~._initialize_data_in_buffer` for details on the data stored
        in the buffer and the integrity checks performed.

        Returns
        -------
        Collection
            A collection of the same base type as the SyncedCollection this
            method is called for, corresponding to data loaded from the
            underlying file.

        """
        super()._load_from_buffer()

        # Set local data to the version in the buffer.
        self._data = type(self)._buffer[self._filename]["contents"]

    def _initialize_data_in_buffer(self, modified=False):
        """Create the initial entry for the data in the cache.

        Stores the following information:
            - The metadata provided by :meth:`~._get_file_metadata`. Used to
              check if a file has been modified on disk since it was loaded
              into the buffer.
            - A flag indicating whether any operation that saves to the buffer
              has occurred, e.g. a ``__setitem__`` call. This flag is used to
              determine what collections need to be saved to disk when
              flushing.

        Parameters
        ----------
        modified : bool
            Whether or not the data has been modified from the version on disk
            (Default value = False).

        """
        metadata = self._get_file_metadata()
        type(self)._buffer[self._filename] = {
            "contents": self._data,
            "metadata": metadata,
            "modified": modified,
        }

    @classmethod
    def _flush_buffer(cls, force=False):
        """Flush the data in the file buffer.

        Parameters
        ----------
        force : bool
            If True, force a flush even in buffered mode (defaults to False). This
            parameter is used when the buffer is filled to capacity.

        Returns
        -------
        dict
            Mapping from filenames to errors that occured while flushing data.

        Raises
        ------
        BufferedError
            If there are any issues with flushing the data.

        """
        return super()._flush_buffer(force=force, retain_in_force=True)
