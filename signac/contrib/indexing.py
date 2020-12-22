# Copyright (c) 2017 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
import errno
import hashlib
import importlib.machinery
import logging
import math
import os
import re
import warnings
from collections import defaultdict
from time import sleep

from deprecation import deprecated

from ..common import errors
from ..core import json
from ..version import __version__
from .hashing import calc_id
from .utility import walkdepth

logger = logging.getLogger(__name__)

KEY_PROJECT = "project"
KEY_FILENAME = "filename"
KEY_PATH = "root"
KEY_PAYLOAD = "format"

"""
THIS MODULE IS DEPRECATED!
"""


@deprecated(
    deprecated_in="1.3",
    removed_in="2.0",
    current_version=__version__,
    details="The indexing module is deprecated.",
)
def md5(file):
    "Calculate and return the md5 hash value for the file data."
    m = hashlib.md5()
    for chunk in iter(lambda: file.read(4096), b""):
        m.update(chunk)
    return m.hexdigest()


def _is_blank_module(module):
    with open(module.__file__) as file:
        return not bool(file.read().strip())


# this class is deprecated
class BaseCrawler:
    """Crawl through `root` and index all files.

    The crawler creates an index on data, which can be exported
    to a database for easier access."""

    tags = None

    @deprecated(
        deprecated_in="1.3",
        removed_in="2.0",
        current_version=__version__,
        details="The indexing module is deprecated.",
    )
    def __init__(self, root):
        """Initialize a BaseCrawler instance.

        :param root: The path to the root directory to crawl through.
        :type root: str"""
        self.root = os.path.expanduser(root)
        self.tags = set() if self.tags is None else set(self.tags)

    def docs_from_file(self, dirpath, fn):
        """Implement this method to generate documents from files.

        :param dirpath: The path of the file, relative to `root`.
        :type dirpath: str
        :param fn: The filename.
        :type fn: str
        :yields: Index documents.
        """
        raise NotImplementedError()
        yield

    def fetch(self, doc, mode="r"):
        """Implement this generator method to associate data with a document.

        :returns: object associated with doc
        """
        raise errors.FetchError(f"Unable to fetch object for '{doc}'.")

    @classmethod
    def _calculate_hash(cls, doc, dirpath, fn):
        blob = json.dumps(doc, sort_keys=True)
        m = hashlib.md5()
        m.update(dirpath.encode("utf-8"))
        m.update(fn.encode("utf-8"))
        m.update(blob.encode("utf-8"))
        return m.hexdigest()

    def crawl(self, depth=0):
        """Crawl through the `root` directory.

        The crawler will inspect every file and directory up
        until the specified `depth` and call the
        :meth:`docs_from_file` method.

        :param depth: Crawl through the directory for the specified depth.
                      A value of 0 specifies no limit.
        :type dept: int
        :yields: (id, doc)-tuples"""
        logger.info(f"Crawling '{self.root}' (depth={depth})...")
        for dirpath, dirnames, filenames in walkdepth(self.root, depth):
            for fn in filenames:
                for doc in self.docs_from_file(dirpath, fn):
                    logger.debug(
                        "doc from file: '{}'.".format(os.path.join(dirpath, fn))
                    )
                    doc.setdefault(KEY_PAYLOAD, None)
                    doc.setdefault("_id", self._calculate_hash(doc, dirpath, fn))
                    yield doc
        logger.info(f"Crawl of '{self.root}' done.")

    def process(self, doc, dirpath, fn):
        """Implement this method for additional processing of generated docs.

        The default implementation will return the unmodified `doc`.

        :param dirpath: The path of the file, relative to `root`.
        :type dirpath: str
        :param fn: The filename.
        :type fn: str
        :returns: A document, that means an instance of mapping.
        :rtype: mapping"""
        return doc


# this class is deprecated
class RegexFileCrawler(BaseCrawler):
    r"""Generate documents from filenames and associate each file with a data type.

    The `RegexFileCrawler` uses regular expressions to generate
    data from files. This is a particular easy method to retrieve meta data
    associated with files. Inherit from this class to configure a crawler
    for your data structre.

    Let's assume we want to index text files, with a naming pattern, that
    specifies a parameter `a` through the filename, e.g.:

    .. code-block:: python

        ~/my_project/a_0.txt
        ~/my_project/a_1.txt
        ...

    A valid regular expression to match this pattern would
    be: ``.*\/a_(?P<a>\d+)\.txt`` which may be defined for a crawler as such:

    .. code-block:: python

        MyCrawler(RegexFileCrawler):
            pass

        MyCrawler.define('.*\/a_(?P<a>\d+)\.txt', 'TextFile')
    """
    "Mapping of compiled regex objects and associated formats."
    definitions = {}  # type: ignore

    @classmethod
    @deprecated(
        deprecated_in="1.3",
        removed_in="2.0",
        current_version=__version__,
        details="The indexing module is deprecated.",
    )
    def define(cls, regex, format_=None):
        """Define a format for a particular regular expression.

        :param regex: All files of the specified format
            must match this regular expression.
        :type regex: :class:`str`
        :param format_: The format associated with all matching files.
        :type format_: :class:`object`
        """
        if isinstance(regex, str):
            regex = re.compile(regex)
        definitions = dict(cls.definitions)
        definitions[regex] = format_
        cls.definitions = definitions

    @classmethod
    @deprecated(
        deprecated_in="1.3",
        removed_in="2.0",
        current_version=__version__,
        details="The indexing module is deprecated.",
    )
    def compute_file_id(cls, doc, file):
        """Compute the file id for a given doc and the associated file.

        :param doc: The index document
        :param file: The associated file
        :returns: The file id.
        """
        file_id = doc["md5"] = md5(file)
        return file_id

    @deprecated(
        deprecated_in="1.3",
        removed_in="2.0",
        current_version=__version__,
        details="The indexing module is deprecated.",
    )
    def docs_from_file(self, dirpath, fn):
        """Generate documents from filenames.

        This method implements the abstract
        :py:meth:~.BaseCrawler.docs_from_file` and yields index
        documents associated with files.

        .. note::
            It is not recommended to reimplement this method to modify
            documents generated from filenames.
            See :meth:`~RegexFileCrawler.process` instead.

        :param dirpath: The path of the file relative to root.
        :param fn: The filename of the file.
        :yields: Index documents.
        """
        for regex, format_ in self.definitions.items():
            m = regex.match(os.path.join(dirpath, fn))
            if m:
                doc = self.process(m.groupdict(), dirpath, fn)
                doc[KEY_FILENAME] = os.path.relpath(
                    os.path.join(dirpath, fn), self.root
                )
                doc[KEY_PATH] = os.path.abspath(self.root)
                doc[KEY_PAYLOAD] = str(format_)
                with open(os.path.join(dirpath, fn), "rb") as file:
                    doc["file_id"] = self.compute_file_id(doc, file)
                yield doc

    @deprecated(
        deprecated_in="1.3",
        removed_in="2.0",
        current_version=__version__,
        details="The indexing module is deprecated.",
    )
    def fetch(self, doc, mode="r"):
        """Fetch the data associated with `doc`.

        :param doc: A index document.
        :type doc: :class:`dict`
        :returns: The file associated with the index document.
        :rtype: A file-like object
        """
        fn = doc.get(KEY_FILENAME)
        if fn:
            for regex, format_ in self.definitions.items():
                ffn = os.path.join(self.root, fn)
                m = regex.match(ffn)
                if m:
                    if isinstance(format_, str):
                        return open(ffn, mode=mode)
                    else:
                        for meth in ("read", "close"):
                            if not callable(getattr(format_, meth, None)):
                                msg = f"Format {format_} has no {meth}() method."
                                warnings.warn(msg)
                        return format_(open(ffn, mode=mode))
            else:
                raise errors.FetchError(
                    f"Unable to match file path of doc '{doc}' to format definition."
                )
        else:
            raise errors.FetchError(f"Insufficient meta data in doc '{doc}'.")

    @deprecated(
        deprecated_in="1.3",
        removed_in="2.0",
        current_version=__version__,
        details="The indexing module is deprecated.",
    )
    def process(self, doc, dirpath, fn):
        """Post-process documents generated from filenames.

        Example:

        .. code-block:: python

            MyCrawler(signac.indexing.RegexFileCrawler):
                def process(self, doc, dirpath, fn):
                    doc['long_name_for_a'] = doc['a']
                    return super(MyCrawler, self).process(doc, dirpath, fn)

        :param dirpath: The path of the file, relative to `root`.
        :type dirpath: str
        :param fn: The filename.
        :type fn: str
        :returns: An index document, that means an instance of mapping.
        :rtype: mapping
        """
        result = {}
        for key, value in doc.items():
            if value is None or isinstance(value, bool):
                result[key] = value
                continue
            try:
                value = float(value)
            except Exception:
                result[key] = value
            else:
                if not math.isnan(value) or math.isinf(value):
                    if float(value) == int(value):
                        result[key] = int(value)
                    else:
                        result[key] = float(value)
        return super().process(result, dirpath, fn)

    @deprecated(
        deprecated_in="1.3",
        removed_in="2.0",
        current_version=__version__,
        details="The indexing module is deprecated.",
    )
    def crawl(self, depth=0):
        if self.definitions:
            yield from super().crawl(depth=depth)
        else:
            return


@deprecated(
    deprecated_in="1.3",
    removed_in="2.0",
    current_version=__version__,
    details="The indexing module is deprecated.",
)
class JSONCrawler(BaseCrawler):
    encoding = "utf-8"
    fn_regex = r".*\.json"

    def docs_from_json(self, doc):
        yield doc

    def docs_from_file(self, dirpath, fn):
        if re.match(self.fn_regex, os.path.join(dirpath, fn)):
            with open(os.path.join(dirpath, fn), "rb") as file:
                doc = json.loads(file.read().decode(self.encoding))
                yield from self.docs_from_json(doc)


def _index_signac_project_workspace(
    root,
    include_job_document=True,
    fn_statepoint="signac_statepoint.json",
    fn_job_document="signac_job_document.json",
    statepoint_index="statepoint",
    signac_id_alias="_id",
    encoding="utf-8",
    statepoint_dict=None,
):
    "Yields standard index documents for a signac project workspace."
    logger.debug(f"Indexing workspace '{root}'...")
    m = re.compile(r"[a-f0-9]{32}")
    try:
        job_ids = [jid for jid in os.listdir(root) if m.match(jid)]
    except OSError as error:
        if error.errno == errno.ENOENT:
            return
        else:
            raise
    for i, job_id in enumerate(job_ids):
        if not m.match(job_id):
            continue
        doc = {"signac_id": job_id, KEY_PATH: root}
        if signac_id_alias:
            doc[signac_id_alias] = job_id
        fn_sp = os.path.join(root, job_id, fn_statepoint)
        with open(fn_sp, "rb") as file:
            sp = json.loads(file.read().decode(encoding))
            if statepoint_dict is not None:
                statepoint_dict[job_id] = sp
            if statepoint_index:
                doc[statepoint_index] = sp
            else:
                doc.update(sp)
        if include_job_document:
            fn_doc = os.path.join(root, job_id, fn_job_document)
            try:
                with open(fn_doc, "rb") as file:
                    doc.update(json.loads(file.read().decode(encoding)))
            except OSError as error:
                if error.errno != errno.ENOENT:
                    raise
        yield doc
    if job_ids:
        logger.debug("Indexed workspace '{}', {} entries.".format(root, i + 1))


# this class is deprecated
class SignacProjectCrawler(RegexFileCrawler):
    """Index a signac project workspace.

    Without any file format definitions, this crawler
    yields index documents for each job, including
    the statepoint and the job document.

    See also: :py:class:`~.RegexFileCrawler`

    :param root: The path to the project's root directory.
    :type root: str"""

    encoding = "utf-8"
    statepoint_index = "statepoint"
    fn_statepoint = "signac_statepoint.json"
    fn_job_document = "signac_job_document.json"
    signac_id_alias = "_id"

    @deprecated(
        deprecated_in="1.3",
        removed_in="2.0",
        current_version=__version__,
        details="The indexing module is deprecated.",
    )
    def __init__(self, root):
        from .project import get_project

        root = get_project(root=root).workspace()
        self._statepoints = {}
        return super().__init__(root=root)

    def _get_job_id(self, dirpath):
        return os.path.relpath(dirpath, self.root).split("/")[0]

    def _read_statepoint(self, job_id):
        fn_sp = os.path.join(self.root, job_id, self.fn_statepoint)
        with open(fn_sp, "rb") as file:
            return json.loads(file.read().decode(self.encoding))

    def _get_statepoint(self, job_id):
        sp = self._statepoints.setdefault(job_id, self._read_statepoint(job_id))
        assert calc_id(sp) == job_id
        return sp

    def get_statepoint(self, dirpath):
        job_id = self._get_job_id(dirpath)
        return job_id, self._get_statepoint(self, job_id)

    def process(self, doc, dirpath, fn):
        if dirpath is not None:
            job_id = self._get_job_id(dirpath)
            statepoint = self._get_statepoint(job_id)
            doc["signac_id"] = job_id
            if self.statepoint_index:
                doc[self.statepoint_index] = statepoint
            else:
                doc.update(statepoint)
        return super().process(doc, dirpath, fn)

    def crawl(self, depth=0):
        for doc in _index_signac_project_workspace(
            root=self.root,
            fn_statepoint=self.fn_statepoint,
            fn_job_document=self.fn_job_document,
            statepoint_index=self.statepoint_index,
            signac_id_alias=self.signac_id_alias,
            encoding=self.encoding,
            statepoint_dict=self._statepoints,
        ):
            yield self.process(doc, None, None)
        for doc in super().crawl(depth=depth):
            yield doc


# this class is deprecated
class MainCrawler(BaseCrawler):
    r"""Compiles a main index from indexes defined in access modules.

    An instance of this crawler will search the data space for access
    modules, which by default are named ``signac_access.py``. Once such
    a file is found, the crawler will import the module and try to execute
    two special functions given that they are defined within the module's
    global namespace: ``get_indexes()`` and ``get_crawlers()``.

    The ``get_indexes()`` is assumed to yield one or multiple index generator
    functions, while the ``get_crawlers()`` function is assumed to yield
    one or more crawler instances.

    This is an example for such an access module:

    .. code-block:: python

        import signac

        def get_indexes(root):
            yield signac.index_files(root, r'.*\.txt')

        def get_crawlers(root):
            yield MyCrawler(root)

    In case that the main crawler has tags, the ``get_indexes()`` function
    will always be ignored while crawlers yielded from the ``get_crawlers()``
    function will only be executed in case that they match at least one
    of the tags.

    In case that the access module is completely empty, it will be executed
    as if it had the following directives:

    .. code-block:: python

        import signac

        def get_indexes(root):
            yield signac.get_project(root).index()

    Tags for indexes yielded from the `get_indexes()` function can be specified
    by assigning them directly to the function:

    .. code-block:: python

        def get_indexes(root):
            yield signac.get_project(root).index()

        get_indexes.tags = {'foo'}


    :param root: The path to the root directory to crawl through.
    :type root: str
    :param raise_on_error: Raise all exceptions encountered during
        during crawling instead of ignoring them.
    :type raise_on_error: bool
    """

    FN_ACCESS_MODULE = "signac_access.py"
    "The filename of modules containing crawler definitions."

    @deprecated(
        deprecated_in="1.3",
        removed_in="2.0",
        current_version=__version__,
        details="The indexing module is deprecated.",
    )
    def __init__(self, root, raise_on_error=False):
        self.raise_on_error = raise_on_error
        super().__init__(root=root)

    def _docs_from_module(self, dirpath, fn):
        name = os.path.join(dirpath, fn)
        module = importlib.machinery.SourceFileLoader(name, name).load_module()

        logger.info(f"Crawling from module '{module.__file__}'.")

        has_tags = self.tags is not None and len(set(self.tags))

        def _check_tags(tags):
            if tags is None or not len(set(tags)):
                if has_tags:
                    logger.info("Skipping, index has no defined tags.")
                    return False
                else:
                    return True
            else:
                if not has_tags:
                    logger.info("Skipping, index requires tags.")
                    return False
                elif set(self.tags).intersection(set(tags)):
                    return True  # at least one tag matches!
                else:
                    logger.info("Skipping, tag mismatch.")
                    return False

        if not has_tags and _is_blank_module(module):
            from .project import get_project

            for doc in get_project(root=dirpath).index():
                yield doc

        if hasattr(module, "get_indexes"):
            if _check_tags(getattr(module.get_indexes, "tags", None)):
                for index in module.get_indexes(dirpath):
                    for doc in index:
                        yield doc

        if hasattr(module, "get_crawlers"):
            for crawler in module.get_crawlers(dirpath):
                logger.info(f"Executing subcrawler:\n {crawler}")
                if _check_tags(getattr(crawler, "tags", None)):
                    for doc in crawler.crawl():
                        doc.setdefault(KEY_PROJECT, os.path.relpath(dirpath, self.root))
                        yield doc

    def docs_from_file(self, dirpath, fn):
        """Compile main index from file in case it is an access module.

        :param dirpath: The path of the file relative to root.
        :param fn: The filename of the file.
        :yields: Index documents.
        """
        if fn == self.FN_ACCESS_MODULE:
            try:
                yield from self._docs_from_module(dirpath, fn)
            except Exception:
                logger.error(
                    "Error while indexing from module '{}'.".format(
                        os.path.join(dirpath, fn)
                    )
                )
                if self.raise_on_error:
                    raise
            else:
                logger.debug(
                    "Completed indexing from '{}'.".format(os.path.join(dirpath, fn))
                )


# Deprecated API
class MasterCrawler(MainCrawler):
    def __init__(self, *args, **kwargs):
        warnings.warn(
            "The MasterCrawler class has been replaced by the MainCrawler class. "
            "Both classes are deprecated and will be removed in a future release.",
            DeprecationWarning,
        )
        super().__init__(*args, **kwargs)


@deprecated(
    deprecated_in="1.3",
    removed_in="2.0",
    current_version=__version__,
    details="The indexing module is deprecated.",
)
def fetch(
    doc_or_id, mode="r", mirrors=None, num_tries=3, timeout=60, ignore_local=False
):
    """Fetch the file associated with this document or file id.

    This function retrieves a file associated with the provided
    index document or file id and behaves like the built-in
    :py:func:`open` function, e.g.:

    .. code-block:: python

        for doc in index:
            with signac.fetch(doc) as file:
                do_something_with(file)

    :param doc_or_id: A file_id or a document with a file_id value.
    :param mode: Mode to use for opening files.
    :param mirrors: An optional set of mirrors to fetch the file from.
    :param num_tries: The number of automatic retry attempts in case of
        mirror connection errors.
    :type num_tries: int
    :param timeout: The time in seconds to wait before an
        automatic retry attempt.
    :type timeout: int
    :returns: The file associated with the document or file id.
    :rtype: A file-like object
    """
    if doc_or_id is None:
        raise ValueError("Argument 'doc_or_id' must not be None!")
    file_id = doc_or_id if isinstance(doc_or_id, str) else doc_or_id.get("file_id")
    if not ignore_local:
        try:
            fn = os.path.join(doc_or_id["root"], doc_or_id["filename"])
            return open(fn, mode=mode)
        except KeyError:
            raise errors.FetchError("Insufficient file meta data for fetch.", doc_or_id)
        except OSError as error:
            if error.errno == errno.ENOENT:
                if file_id is None:
                    raise errors.FetchError(f"Failed to fetch '{doc_or_id}'.")
    if mirrors is None:
        raise errors.FetchError("No mirrors provided!")
    else:
        for i in range(num_tries):
            for mirror in mirrors:
                try:
                    return mirror.get(file_id, mode=mode)
                except mirror.AutoRetry as error:
                    logger.warning(error)
                    sleep(timeout)
                except mirror.FileNotFoundError as error:
                    logger.debug(error)
            else:
                raise errors.FetchError(f"Unable to fetch object for '{file_id}'.")


@deprecated(
    deprecated_in="1.3",
    removed_in="2.0",
    current_version=__version__,
    details="The indexing module is deprecated.",
)
def fetched(docs):
    """Iterate over documents and yield associated files."""
    for doc in docs:
        if "file_id" in doc:
            yield doc, fetch(doc)


def _export_to_mirror(file, file_id, mirror):
    "Export a file-like object with file_id to mirror."
    with mirror.new_file(_id=file_id) as dst:
        dst.write(file.read())


@deprecated(
    deprecated_in="1.3",
    removed_in="2.0",
    current_version=__version__,
    details="The indexing module is deprecated.",
)
def export_to_mirror(doc, mirror, num_tries=3, timeout=60):
    """Export a file associated with doc to mirror.

    :param doc: A document with a file_id entry.
    :param mirror: A file-system object to export the file to.
    :param num_tries: The number of automatic retry attempts in case of
        mirror connection errors.
    :type num_tries: int
    :param timeout: The time in seconds to wait before an
        automatic retry attempt.
    :type timeout: int
    :returns: The file id after successful export.
    """
    if "file_id" not in doc:
        raise errors.ExportError(f"Doc '{doc}' does not have a file_id entry.")
    for i in range(num_tries):
        try:
            with fetch(doc, mode="rb") as file:
                _export_to_mirror(file, doc["file_id"], mirror)
        except mirror.FileExistsError:
            logger.debug(
                "File with id '{}' already exported, skipping.".format(doc["file_id"])
            )
            break
        except mirror.AutoRetry as error:
            logger.warning(f"Error during export: '{error}', retrying...")
            sleep(timeout)
        else:
            logger.debug(
                "Stored file with id '{}' in mirror '{}'.".format(
                    doc["file_id"], mirror
                )
            )
            return doc["file_id"]
    else:
        raise errors.ExportError(doc)


@deprecated(
    deprecated_in="1.3",
    removed_in="2.0",
    current_version=__version__,
    details="The indexing module is deprecated.",
)
def export_one(doc, index, mirrors=None, num_tries=3, timeout=60):
    """Export one document to index and an optionally associated file to mirrors.

    :param doc: A document with a file_id entry.
    :param docs: The index collection to export to.
    :param mirrors: An optional set of mirrors to export files to.
    :param num_tries: The number of automatic retry attempts in case of
        mirror connection errors.
    :type num_tries: int
    :param timeout: The time in seconds to wait before an
        automatic retry attempt.
    :type timeout: int
    :returns: The id and file id after successful export.
    """
    index.replace_one({"_id": doc["_id"]}, doc, upsert=True)
    if mirrors and "file_id" in doc:
        for mirror in mirrors:
            export_to_mirror(doc, mirror, num_tries, timeout)
        return doc["_id"], doc["file_id"]
    else:
        return doc["_id"], None


@deprecated(
    deprecated_in="1.3",
    removed_in="2.0",
    current_version=__version__,
    details="The indexing module is deprecated.",
)
def export(docs, index, mirrors=None, update=False, num_tries=3, timeout=60, **kwargs):
    """Export docs to index and optionally associated files to mirrors.

    The behavior of this function is equivalent to:

    .. code-block:: python

        for doc in docs:
            export_one(doc, index, mirrors, num_tries)

    If the `update` argument is set to True, the export algorithm will
    automatically identify stale index documents, that means documents
    that refer to files or state points that have been removed and are
    no longer part of the data space. Any document which shares the
    `root`, but not the `_id` field with any of the updated documents
    is considered stale and removed. Using `update` in combination with
    an empty docs sequence will raise `ExportError`, since it is not
    possible to identify stale documents in that case.

    .. note::

        This function will automatically delegate to specialized
        implementations for special index types. For example, if
        the index argument is a MongoDB document collection, the
        index documents will be exported via :py:func:`~.export_pymongo`.

    :param docs: The index documents to export.
    :param index: The collection to export the index to.
    :param mirrors: An optional set of mirrors to export files to.
    :param update: If True, remove stale index documents, that means
        documents that refer to files or state points that no longer exist.
    :type update: bool
    :param num_tries: The number of automatic retry attempts in case of
        mirror connection errors.
    :type num_tries: int
    :param timeout: The time in seconds to wait before an
        automatic retry attempt.
    :type timeout: int
    :param kwargs: Optional keyword arguments to pass to
        delegate implementations.
    :raises ExportError: When using the update argument in combination with
        an empty docs sequence.
    """
    try:
        import pymongo
    except ImportError:
        pass
    else:
        if isinstance(index, pymongo.collection.Collection):
            logger.info("Using optimized export function export_pymongo().")
            return export_pymongo(
                docs=docs,
                index=index,
                mirrors=mirrors,
                update=update,
                num_tries=num_tries,
                timeout=timeout,
                **kwargs,
            )
    ids = defaultdict(list)
    for doc in docs:
        _id, _ = export_one(doc, index, mirrors, num_tries, timeout, **kwargs)
        if update:
            root = doc.get("root")
            if root is not None:
                ids[root].append(_id)
    if update:
        if ids:
            stale = set()
            for root in ids:
                docs_ = index.find({"root": root})
                all_ = {doc["_id"] for doc in docs_}
                stale.update(all_.difference(ids[root]))
            logger.info("Removing {} stale documents.".format(len(stale)))
            for _id in set(stale):
                index.delete_one(dict(_id=_id))
        else:
            raise errors.ExportError(
                "The exported docs sequence is empty! Unable to update!"
            )


def _export_pymongo(docs, operations, index, mirrors, num_tries, timeout):
    """Export docs via operations to index and files to mirrors."""
    import pymongo

    if mirrors is not None:
        for mirror in mirrors:
            for doc in docs:
                if "file_id" in doc:
                    export_to_mirror(doc, mirror, num_tries, timeout)
    for i in range(num_tries):
        try:
            index.bulk_write(operations)
            break
        except pymongo.errors.AutoReconnect as error:
            logger.warning(error)
            sleep(timeout)
    else:
        raise errors.ExportError()


@deprecated(
    deprecated_in="1.3",
    removed_in="2.0",
    current_version=__version__,
    details="The indexing module is deprecated.",
)
def export_pymongo(
    docs, index, mirrors=None, update=False, num_tries=3, timeout=60, chunksize=100
):
    """Optimized :py:func:`~.export` function for pymongo index collections.

    The behavior of this function is rougly equivalent to:

    .. code-block:: python

        for doc in docs:
            export_one(doc, index, mirrors, num_tries)

    .. note::

        All index documents must be JSON-serializable to
        be able to be exported to a MongoDB collection.

    :param docs: The index documents to export.
    :param index: The database collection to export the index to.
    :type index: :class:`pymongo.collection.Collection`
    :param num_tries: The number of automatic retry attempts in case of
        mirror connection errors.
    :type num_tries: int
    :param timeout: The time in seconds to wait before an
        automatic retry attempt.
    :type timeout: int
    :param chunksize: The buffer size for export operations.
    :type chunksize: int"""
    import pymongo

    logger.info(f"Exporting to pymongo database collection index '{index}'.")
    chunk = []
    operations = []
    ids = defaultdict(list)
    for doc in docs:
        f = {"_id": doc["_id"]}
        if update:
            root = doc.get("root")
            if root is not None:
                ids[root].append(doc["_id"])
        chunk.append(doc)
        operations.append(pymongo.ReplaceOne(f, doc, upsert=True))
        if len(chunk) >= chunksize:
            logger.debug("Pushing chunk.")
            _export_pymongo(chunk, operations, index, mirrors, num_tries, timeout)
            chunk[:] = []
            operations[:] = []
    if len(operations):
        logger.debug("Pushing final chunk.")
        _export_pymongo(chunk, operations, index, mirrors, num_tries, timeout)
    if update:
        if ids:
            stale = set()
            for root in ids:
                docs_ = index.find({"root": root})
                all_ = {doc["_id"] for doc in docs_}
                stale.update(all_.difference(ids[root]))
            logger.info("Removing {} stale documents.".format(len(stale)))
            for _id in set(stale):
                index.delete_one(dict(_id=_id))
        else:
            raise errors.ExportError(
                "The exported docs sequence is empty! Unable to update!"
            )


@deprecated(
    deprecated_in="1.3",
    removed_in="2.0",
    current_version=__version__,
    details="The indexing module is deprecated.",
)
def index_files(root=".", formats=None, depth=0):
    r"""Generate a file index.

    This generator function yields file index documents,
    where each index document corresponds to one file.

    To index all files in the current working directory,
    simply execute:

    .. code-block:: python

        for doc in signac.index_files():
            print(doc)

    A file associated with a file index document can be
    fetched via the :py:func:`fetch` function:

    .. code-block:: python

        for doc in signac.index_files():
            with signac.fetch(doc) as file:
                print(file.read())

    This is especially useful if the file index is part of
    a collection (:py:class:`.Collection`) which can be searched
    for specific entries.

    To limit the file index to files with a specific filename
    formats, provide a regular expression as the formats argument.
    To index all files that have file ending `.txt`, execute:

    .. code-block:: python

        for doc in signac.index_files(formats='.*\.txt'):
            print(doc)

    We can specify specific formats by providing a dictionary as
    ``formats`` argument, where the key is the filename pattern and
    the value is an arbitrary formats string, e.g.:

    .. code-block:: python

        for doc in signac.index_files(formats=
            {r'.*\.txt': 'TextFile', r'.*\.zip': 'ZipFile'}):
            print(doc)

    :param root: The directory to index, defaults to the
        current working directory.
    :type root: str
    :param formats: Limit the index to files that match the
        given regular expression and optionally associate formats
        with given patterns.
    :param depth: Limit the search to the specified directory depth.
    :type depth: int
    :yields: The file index documents as dicts.
    """
    if formats is None:
        formats = {".*": "File"}
    if isinstance(formats, str):
        formats = {formats: "File"}

    class Crawler(RegexFileCrawler):
        pass

    for regex, fmt in formats.items():
        Crawler.define(regex, fmt)

    yield from Crawler(root).crawl(depth=depth)


@deprecated(
    deprecated_in="1.3",
    removed_in="2.0",
    current_version=__version__,
    details="The indexing module is deprecated.",
)
def index(root=".", tags=None, depth=0, **kwargs):
    r"""Generate a main index.

    A main index is compiled from other indexes by searching
    for modules named ``signac_access.py`` and compiling all
    indexes which are yielded from a function ``get_indexes(root)``
    defined within that module as well as the indexes generated by
    crawlers yielded from a function ``get_crawlers(root)`` defined
    within that module.

    This is a minimal example for a ``signac_access.py`` file:

    .. code-block:: python

        import signac

        def get_indexes(root):
            yield signac.index_files(root, r'.*\.txt')

    Internally, this function constructs an instance of
    :py:class:`.MainCrawler` and all extra key-word arguments
    will be forwarded to the constructor of said main crawler.

    :param root: Look for access modules under this directory path.
    :type root: str
    :param tags: If tags are provided, do not execute subcrawlers
        that don't match the same tags.
    :param depth: Limit the search to the specified directory depth.
    :param kwargs: These keyword-arguments are forwarded to the
        internal MainCrawler instance.
    :type depth: int
    :yields: The main index documents as instances of dict.
    """

    class Crawler(MainCrawler):
        pass

    if tags is not None:
        Crawler.tags = tags

    yield from Crawler(root, **kwargs).crawl(depth=depth)
