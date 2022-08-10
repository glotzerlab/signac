# Copyright (c) 2020 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Buffering for file-based backends using a serialized buffer.

The buffering method implemented here involves a single buffer of serialized
data. All collections in buffered mode encode their data into this buffer on save
and decode from it on load.
"""

import hashlib
import json

from ..errors import MetadataError
from ..utils import SyncedCollectionJSONEncoder
from .file_buffered_collection import FileBufferedCollection


class SerializedFileBufferedCollection(FileBufferedCollection):
    """A :class:`~.FileBufferedCollection` based on a serialized data store.

    This class extends the :class:`~.FileBufferedCollection` and implements a
    concrete storage mechanism in which data is encoded (by default, into JSON)
    and stored into a buffer. This buffer functions as a central data store for
    all collections and is a synchronization point for various collections
    pointing to the same underlying file. This serialization method may be a
    bottleneck in some applications; see the Warnings section for more information.

    The buffer size and capacity for this class is measured in the total number
    of bytes stored in the buffer that correspond to file data. This is *not*
    the total size of the buffer, which also contains additional information
    like the hash of the data and the file metadata (which are used for
    integrity checks), but it is the relevant metric for users.

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
    from triggering a flush while another still thinks its data is in the cache.

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
    - The overhead of this buffering method is quite high due to the constant
      encoding and decoding of data. For performance-critical applications where
      memory is not highly constrained and virtual memory limits are absent, the
      :class:`~.SharedMemoryFileBufferedCollection` may be more appropriate.
    - Due to the possibility of read operations triggering a flush, the
      contents of the buffer may be invalidated on loads as well. To prevent this
      even nominally read-only operations are serialized. As a result, although
      this class is thread safe, it will effectively serialize all operations and
      will therefore not be performant.

    """

    _BUFFER_CAPACITY = 32 * 2**20  # 32 MB

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
        with self._buffer_lock:
            if not self._is_buffered or force:
                try:
                    cached_data = type(self)._buffer[self._filename]
                except KeyError:
                    # There are valid reasons for nothing to be in the cache (the
                    # object was never actually accessed during global buffering,
                    # multiple collections pointing to the same file, etc).
                    return
                else:
                    blob = self._encode(self._data)

                    # If the contents have not been changed since the initial read,
                    # we don't need to rewrite it.
                    try:
                        if self._hash(blob) != cached_data["hash"]:
                            # Validate that the file hasn't been changed by
                            # something else.
                            if cached_data["metadata"] != self._get_file_metadata():
                                raise MetadataError(
                                    self._filename, cached_data["contents"]
                                )
                            self._update(self._decode(cached_data["contents"]))
                            self._save_to_resource()
                    finally:
                        # Whether or not an error was raised, the cache must be
                        # cleared to ensure a valid final buffer state.
                        del type(self)._buffer[self._filename]
                        data_size = len(cached_data["contents"])
                        type(self)._CURRENT_BUFFER_SIZE -= data_size

    @staticmethod
    def _hash(blob):
        """Calculate and return the md5 hash value for the file data.

        Parameters
        ----------
        blob : bytes
            Byte literal to be hashed.

        Returns
        -------
        str
            The md5 hash of the input bytes.

        """
        if blob is not None:
            m = hashlib.md5()
            m.update(blob)
            return m.hexdigest()

    @staticmethod
    def _encode(data):
        """Encode the data into a serializable form.

        This method assumes JSON-serializable data, but subclasses can override
        this hook method to change the encoding behavior as needed.

        Parameters
        ----------
        data : collections.abc.Collection
            Any collection type that can be encoded.

        Returns
        -------
        bytes
            The underlying encoded data.

        """
        return json.dumps(data, cls=SyncedCollectionJSONEncoder).encode()

    @staticmethod
    def _decode(blob):
        """Decode serialized data.

        This method assumes JSON-serializable data, but subclasses can override
        this hook method to change the encoding behavior as needed.

        Parameters
        ----------
        blob : bytes
            Byte literal to be decoded.

        Returns
        -------
        data : collections.abc.Collection
            The decoded data in the appropriate base collection type.

        """
        return json.loads(blob.decode())

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
                blob = self._encode(self._data)
                cached_data = type(self)._buffer[self._filename]
                buffer_size_change = len(blob) - len(cached_data["contents"])
                type(self)._CURRENT_BUFFER_SIZE += buffer_size_change
                cached_data["contents"] = blob
            else:
                # The only methods that could safely call sync without a load are
                # destructive operations like `reset` or `clear` that completely
                # wipe out previously existing data. Therefore, the safest choice
                # for ensuring consistency of the buffer is to modify the stored
                # hash (which is used for the consistency check) with the hash of
                # the current data on disk. _initialize_data_in_buffer always uses
                # the current metadata, so the only extra work here is to modify
                # the hash after it's called (since it uses self._to_base() to get
                # the data to initialize the cache with).
                self._initialize_data_in_buffer()
                disk_data = self._load_from_resource()
                type(self)._buffer[self._filename]["hash"] = self._hash(
                    self._encode(disk_data)
                )

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
        with self._buffer_lock:
            super()._load_from_buffer()

            # Load from buffer. This has to happen inside the locked context
            # because otherwise the data could be flushed from the buffer by
            # another thread.
            blob = type(self)._buffer[self._filename]["contents"]

        if type(self)._CURRENT_BUFFER_SIZE > type(self)._BUFFER_CAPACITY:
            type(self)._flush_buffer(force=True)
        return self._decode(blob)

    def _initialize_data_in_buffer(self):
        """Create the initial entry for the data in the cache.

        Stores the following information:
            - The hash of the data as initially stored in the cache. This hash
              is used to quickly determine whether data has changed when flushing.
            - The metadata provided by :meth:`~._get_file_metadata`. Used to
              check if a file has been modified on disk since it was loaded
              into the buffer.

        This method also increments the current buffer size, which in this class
        is the total number of bytes of data in the buffer.
        """
        blob = self._encode(self._data)
        metadata = self._get_file_metadata()

        type(self)._buffer[self._filename] = {
            "contents": blob,
            "hash": self._hash(blob),
            "metadata": metadata,
        }
        type(self)._CURRENT_BUFFER_SIZE += len(
            type(self)._buffer[self._filename]["contents"]
        )

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
        return super()._flush_buffer(force=force, retain_in_force=False)
