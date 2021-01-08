# Copyright (c) 2020 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""A standardized buffering implementation for file-based backends.

This module defines a variant of the buffering protocol in
:py:mod:`file_buffered_collection`. This module exploits the same idea that
all file-based backends can use a similar buffering protocol using integrity
checks based on file modification times. However, that buffering approach
mitigates I/O costs by serializing data into a single buffer that is then
treated as the underlying data store by associated :class:`SyncedCollection` objects.
That approach is expensive because it still requires constant encoding and
decoding of data while checking for changes to the data in the buffer from other
instances pointing to the same underlying files. The approach in this module circumvents
this these performance bottlenecks by directly sharing the underlying data store
in the buffer for all objects, completely removing the need for encoding, decoding,
and updating in place.
"""


from .errors import MetadataError
from .file_buffered_collection import FileBufferedCollection


class SharedMemoryFileBufferedCollection(FileBufferedCollection):
    """A :class:`SyncedCollection` that defers all I/O when buffered.

    This class implements a variant of the buffering strategy defined in the
    :class:`signac.core.synced_collections.file_buffered_collection.FileBufferedCollection`.
    Like that class, this class enables buffering for file-based backends by storing
    the last known modification time of the data on disk prior to entering buffered
    mode, then checking whether that has changed when the buffer is flushed.
    The buffering implemented by :class:`~.FileBufferedCollection`
    is true buffering in the sense that the entire write operation is
    performed as normal, including serialization, except that data is written to
    a different persistent store than the underlying resource. However, such
    buffering incurs significant overhead associated all the non-I/O tasks
    required prior to writing to the buffer, particularly data serialization.
    This class exists to remove these performance bottlenecks, which can be
    severe, and provide a more performant alternative.

    The buffering method implemented in this class circumvents the
    aforementioned performance bottlenecks by directly sharing the data between
    multiple synced collections. Rather than encoding and decoding data, all
    objects associated with a file share a single underlying in-memory data store,
    allowing any changes to one to be transparently persisted to the others. When
    the first collection associated with a particular file is accessed or
    modified in buffered mode for the first time, its data is stored in a cache.
    When future collections pointing to the same file access the cache, rather
    than synchronizing their data with the contents of the cache, the underlying
    data attribute of the collection is simply repointed at the data in the cache.
    This method exploits the fact that all mutable collection types in Python are
    references, so modifying one such collection results in modifying all of them,
    thereby removing any need for more complicated synchronization protocols.

    This approach has one principal downside relative to the other buffering
    method. Since the data is not always encoded into a byte-string, the exact
    size of the data in the buffer is not exactly known. This method simply counts
    the number of objects in the buffer as a means to decide how much to store before
    the buffer is flushed. To further simplify the logic and improve performance,
    buffer flushes in this class never remove data from the buffer when forcing
    a flush, they simply write out changes that occurred. The buffer is only cleared
    when a buffered context is exited. This allows storing an arbitrary number of
    objects in the buffer for read-only purposes.

    .. note::
        Important note for subclasses: This class should be inherited before
        any other collections. This requirement is due to the extensive use of
        multiple inheritance: since this class is designed to be combined with
        other :class:`SyncedCollection` types without making those types aware
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
      use of the :class:~.FileBufferedCollection` will allow limiting the total
      memory usage of the process.

    """

    _BUFFER_CAPACITY = 1000  # The number of collections to store in the buffer.

    def _flush(self, force=False):
        """Save buffered changes to the underlying file.

        Parameters
        ----------
        force : bool
            If True, force a flush even in buffered mode (defaults to False). This
            parameter is used when the buffer is filled to capacity.
        stored_metadata : tuple[int, float]
            The metadata stored for this particular collection in the cache. It
            will have been removed by _flush_buffer when this method is called, so
            it must be passed as an argument. Under normal circumstances it will be
            equal to the metadata stored in _cache, but if there are multiple objects
            pointing to the same file and one of them modifies it by exiting buffered
            mode first and the other makes _no_ modifications, it is safe to simply
            reload the data.

        Raises
        ------
        MetadataError
            If any file is detected to have changed on disk since it was
            originally loaded into the buffer and modified.

        """
        if not self._is_buffered or force:
            try:
                cached_data = self._cache[self._filename]
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
                    self._data = self._load_from_resource()
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
                        del self._cache[self._filename]
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
            with self._suspend_sync():
                self._data = {
                    key: self._from_base(data=value, parent=self)
                    for key, value in self._to_base().items()
                }

    def _load(self):
        """Load data from the backend but buffer if needed.

        Override the base buffered method to skip the _update and to let
        _load_from_buffer happen "in place."
        """
        if self._suspend_sync_ <= 0:
            if self._parent is None:
                if self._is_buffered:
                    self._load_from_buffer()
                else:
                    data = self._load_from_resource()
                    with self._suspend_sync():
                        self._update(data)
            else:
                self._parent._load()

    def _save_to_buffer(self):
        """Store data in buffer.

        See :meth:`~._initialize_data_in_cache` for details on the data stored
        in the buffer and the integrity checks performed.
        """
        # Since one object could write to the buffer and trigger a flush while
        # another object was found in the buffer and attempts to proceed
        # normally, we have to serialize this whole block. In theory we might
        # be safe without it because the only operations that should reach this
        # point without already being locked are destructive operations (clear,
        # reset) that don't use the :meth:`_load_and_save` context, and for
        # those the writes will be automatically serialized because Python
        # dicts are thread-safe because of the GIL. However, it's best not to
        # depend on the thread-safety of built-in containers.
        with self._buffer_lock():
            if self._filename in self._cache:
                # Always track all instances pointing to the same data.
                type(self)._cached_collections[id(self)] = self

                # If all we had to do is set the flag, it could be done without any
                # check, but we also need to increment the number of modified
                # items, so we may as well do the update conditionally as well.
                if not self._cache[self._filename]["modified"]:
                    self._cache[self._filename]["modified"] = True
                    type(self)._CURRENT_BUFFER_SIZE += 1
            else:
                self._initialize_data_in_cache(modified=True)
                type(self)._CURRENT_BUFFER_SIZE += 1

            if type(self)._CURRENT_BUFFER_SIZE > type(self)._BUFFER_CAPACITY:
                type(self)._flush_buffer(force=True)

    def _load_from_buffer(self):
        """Read data from buffer.

        See :meth:`~._initialize_data_in_cache` for details on the data stored
        in the buffer and the integrity checks performed.

        Returns
        -------
        Collection
            A collection of the same base type as the SyncedCollection this
            method is called for, corresponding to data loaded from the
            underlying file.

        """
        # Since one object could write to the buffer and trigger a flush while
        # another object was found in the buffer and attempts to proceed
        # normally, we have to serialize this whole block.
        if self._filename in type(self)._cache:
            # Need to check if we have multiple collections pointing to the
            # same file, and if so, track it.
            type(self)._cached_collections[id(self)] = self
        else:
            # The first time this method is called, if nothing is in the buffer
            # for this file then we cannot guarantee that the _data attribute
            # is valid either since the resource could have been modified
            # between when _data was last updated and when this load is being
            # called. As a result, we have to load from the resource here to be
            # safe.
            data = self._load_from_resource()
            with self._thread_lock():
                with self._suspend_sync():
                    self._update(data)
            self._initialize_data_in_cache(modified=False)

        # Set local data to the version in the buffer.
        self._data = self._cache[self._filename]["contents"]

    def _initialize_data_in_cache(self, modified):
        """Create the initial entry for the data in the cache.

        This method should be called the first time that a collection's data is
        accessed in buffered mode. This method stores the encoded data in the
        cache, along with a hash of the data and the metadata of the underlying
        file. The hash is later used for quick checks of whether the data in
        memory has changed since the initial load into the buffer. If so the
        metadata is used to verify that the file on disk has not been modified
        since the data was modified in memory.

        We also maintain a separate set of all collections that are currently
        in buffered mode. This extra storage is necessary because when leaving
        a buffered context we need to make sure to only flush collections that
        are no longer in buffered mode in cases of nested buffering. This
        additional check helps prevent or transparently error on otherwise
        unsafe access patterns.
        """
        metadata = self._get_file_metadata()
        type(self)._cache[self._filename] = {
            "contents": self._data,
            "metadata": metadata,
            "modified": modified,
        }
        type(self)._cached_collections[id(self)] = self

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
            Mapping of filename and errors occured during flushing data.

        Raises
        ------
        BufferedError
            If there are any issues with flushing the data.

        """
        issues = {}

        # We need to use the list of buffered objects rather than directly
        # looping over the local cache so that each collection can
        # independently decide whether or not to flush based on whether it's
        # still buffered (if buffered contexts are nested).
        remaining_collections = {}
        while True:
            with cls._BUFFER_LOCK:
                try:
                    (
                        col_id,
                        collection,
                    ) = cls._cached_collections.popitem()
                except KeyError:
                    break

            # If force is true, the collection must still be buffered, and we
            # want to put it back in the remaining_collections list after
            # flushing any writes. If force is false, then the only way for the
            # collection to still be buffered is if there are nested buffered
            # contexts. In that case, flush_buffer was called due to the exit
            # of an inner buffered context, and we shouldn't do anything with
            # this object, so we just put it back in the list *and* skip the
            # flush.
            if collection._is_buffered and not force:
                remaining_collections[col_id] = collection
                continue
            elif force:
                remaining_collections[col_id] = collection

            try:
                collection._flush(force=force)
            except (OSError, MetadataError) as err:
                issues[collection._filename] = err
        if not issues:
            cls._cached_collections = remaining_collections
        return issues
