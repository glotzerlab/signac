# Copyright (c) 2017 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
import errno
import hashlib
import json
import logging
import math
import os
import re
import warnings

from ..common import errors
from .hashing import calc_id
from .utility import walkdepth

logger = logging.getLogger(__name__)

KEY_PROJECT = "project"
KEY_FILENAME = "filename"
KEY_PATH = "root"
KEY_PAYLOAD = "format"


def _compute_file_md5(file):
    "Calculate and return the md5 hash value for the file data."
    m = hashlib.md5()
    for chunk in iter(lambda: file.read(4096), b""):
        m.update(chunk)
    return m.hexdigest()


class _BaseCrawler:
    """Crawl through `root` and index all files.

    The crawler creates an index on data, which can be exported
    to a database for easier access."""

    tags = None

    def __init__(self, root):
        """Initialize a _BaseCrawler instance.

        :param root: The path to the root directory to crawl through.
        :type root: str
        """
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
        :type depth: int
        :yields: (id, doc)-tuples
        """
        logger.info(f"Crawling '{self.root}' (depth={depth})...")
        for dirpath, dirnames, filenames in walkdepth(self.root, depth):
            for fn in filenames:
                for doc in self.docs_from_file(dirpath, fn):
                    logger.debug(f"doc from file: '{os.path.join(dirpath, fn)}'.")
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
        :rtype: mapping
        """
        return doc


class _RegexFileCrawler(_BaseCrawler):
    r"""Generate documents from filenames and associate each file with a data type.

    The `_RegexFileCrawler` uses regular expressions to generate
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

        MyCrawler(_RegexFileCrawler):
            pass

        MyCrawler.define('.*\/a_(?P<a>\d+)\.txt', 'TextFile')
    """

    "Mapping of compiled regex objects and associated formats."
    definitions = {}  # type: ignore

    @classmethod
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
    def compute_file_id(cls, doc, file):
        """Compute the file id for a given doc and the associated file.

        :param doc: The index document
        :param file: The associated file
        :returns: The file id.
        """
        file_id = doc["md5"] = _compute_file_md5(file)
        return file_id

    def docs_from_file(self, dirpath, fn):
        """Generate documents from filenames.

        This method implements the abstract
        :py:meth:~._BaseCrawler.docs_from_file` and yields index
        documents associated with files.

        .. note::
            It is not recommended to reimplement this method to modify
            documents generated from filenames.
            See :meth:`~_RegexFileCrawler.process` instead.

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

    def process(self, doc, dirpath, fn):
        """Post-process documents generated from filenames.

        Example:

        .. code-block:: python

            MyCrawler(signac.indexing._RegexFileCrawler):
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

    def crawl(self, depth=0):
        if self.definitions:
            yield from super().crawl(depth=depth)
        else:
            return


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
        logger.debug(f"Indexed workspace '{root}', {i + 1} entries.")


class _SignacProjectCrawler(_RegexFileCrawler):
    """Index a signac project workspace.

    Without any file format definitions, this crawler yields index documents for
    each job, including the state point and the job document.

    See also: :py:class:`~._RegexFileCrawler`

    :param root: The path to the project's root directory.
    :type root: str
    """

    encoding = "utf-8"
    statepoint_index = "statepoint"
    fn_statepoint = "signac_statepoint.json"
    fn_job_document = "signac_job_document.json"
    signac_id_alias = "_id"

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
