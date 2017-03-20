# Copyright (c) 2017 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
import sys
import io
import logging
from collections import defaultdict

from ..core.json import json
from ..common import six
if six.PY2:
    from collections import Mapping
else:
    from collections.abc import Mapping


logger = logging.getLogger(__name__)


def _index(docs, key):
    return {doc[key]: doc for doc in docs}


def _flatten(container):
    for i in container:
        if isinstance(i, (list, tuple)):
            for j in _flatten(i):
                yield j
        else:
            yield i


def _encode_tree(x):
    if isinstance(x, list):
        return json.dumps(x)
    else:
        return x


def _traverse_tree(t, include=None, encode=None):
    if encode is not None:
        t = encode(t)
    if include is False:
        return
    if isinstance(t, list):
        for i in t:
            for b in _traverse_tree(i, include, encode):
                yield b
    elif isinstance(t, Mapping):
        for k in t:
            if include is None or include is True:
                for i in _traverse_tree(t[k], encode=encode):
                    yield k, i
            else:
                if not include.get(k, False):
                    continue
                for i in _traverse_tree(t[k], include.get(k), encode=encode):
                    yield k, i
    else:
        yield t


def _traverse_filter(t, include=None):
    for b in _traverse_tree(t, include=include, encode=_encode_tree):
        yield b


def _valid_filter(f, top=True):
    if isinstance(f, Mapping):
        return all(_valid_filter(v, top=False) for v in f.values())
    elif isinstance(f, list):
        return not top
    else:
        return True


def _build_index(docs, key, primary_key):
    nodes = key.split('.')
    index = defaultdict(set)

    def _get_value(doc, nodes):
        if nodes:
            return _get_value(doc[nodes[0]], nodes[1:])
        else:
            return doc

    for doc in docs:
        v = _get_value(doc, nodes)
        index[_encode_tree(v)].add(doc[primary_key])
    return index


class _CollectionSearchResults(object):

    def __init__(self, collection, _ids):
        self._collection = collection
        self._ids = _ids

    def __iter__(self):
        return (self._collection[_id] for _id in self._ids)

    def __len__(self):
        return len(self._ids)

    count = __len__


class Collection(object):
    primary_key = '_id'

    def __init__(self, docs=None):
        self._file = io.StringIO()
        self._dirty = set()
        self._indeces = dict()
        if docs is None:
            self._docs = dict()
        else:
            self._docs = _index(docs, self.primary_key)

    def _assert_open(self):
        if self._docs is None:
            raise RuntimeError("Trying to access closed {}.".format(
                type(self).__name__))

    def _remove_from_indeces(self, _id):
        for index in self._indeces.values():
            remove_keys = set()
            for key, group in index.items():
                try:
                    group.remove(_id)
                except KeyError:
                    pass
                if not len(group):
                    remove_keys.add(key)
            for key in remove_keys:
                del index[key]

    def _update_indeces(self):
        if self._dirty:
            for _id in self._dirty:
                self._remove_from_indeces(_id)
            docs = [self[_id] for _id in self._dirty]
            for key in self._indeces:
                self._indeces[key].update(_build_index(docs, key, self.primary_key))
            self._dirty.clear()

    def _build_index(self, key):
        logger.debug("Building index for key '{}'...".format(key))
        self._indeces[key] = _build_index(self._docs.values(), key, self.primary_key)
        logger.debug("Built index for key '{}'.".format(key))

    def index(self, key, build=False):
        if key == self.primary_key:
            raise KeyError("Can't access index for primary key via index() method.")
        elif key not in self._indeces:
            if build:
                self._build_index(key)
            else:
                raise KeyError("No index for key '{}'.".format(key))
        self._update_indeces()
        return self._indeces[key]

    def __str__(self):
        return "<{} file={}>".format(type(self).__name__, self._file)

    def __iter__(self):
        self._assert_open()
        return iter(self._docs)

    def __len__(self):
        self._assert_open()
        return len(self._docs)

    def __contains__(self, _id):
        return _id in self._docs

    def __getitem__(self, _id):
        self._assert_open()
        return self._docs[_id].copy()

    def __setitem__(self, _id, doc):
        self._assert_open()
        if not isinstance(_id, int):
            raise TypeError("The primary key must be an integer type!")
        doc.setdefault(self.primary_key, _id)
        if doc[self.primary_key] != _id:
            raise ValueError("Primary key ('{}') mismatch!".format(self.primary_key))
        doc = json.loads(json.dumps(doc))
        self._dirty.add(_id)
        self._docs[_id] = doc

    def __delitem__(self, _id):
        self._assert_open()
        del self._docs[_id]
        self._remove_from_indeces(_id)
        try:
            self._dirty.remove(_id)
        except KeyError:
            pass

    def clear(self):
        self._docs.clear()
        self._indeces.clear()
        self._dirty.clear()

    def update(self, docs):
        for doc in docs:
            doc.setdefault(self.primary_key, len(self))
            self[doc[self.primary_key]] = doc

    def _check_filter(self, filter):
        if filter is None:
            return True
        if not _valid_filter(filter):
            raise ValueError(filter)

    def _find(self, filter=None):
        filter = json.loads(json.dumps(filter))  # Normalize
        self._check_filter(filter)
        if filter is None or not len(filter):
            return self._docs.keys()
        _id = filter.pop(self.primary_key, None)
        if _id is not None and _id in self:
            result = {_id}
        else:
            result = None
        for branch in _traverse_filter(filter):
            nodes = list(_flatten(branch))
            key = '.'.join(nodes[:-1])
            value = nodes[-1]
            index = self.index(key, build=True)
            matches = index.get(value, set())
            if result is None:
                result = matches
            else:
                result = result.intersection(matches)
            if not result:
                break
        return result

    def find(self, filter=None):
        """Find all documents matching filter."""
        return _CollectionSearchResults(self, self._find(filter))

    def replace_one(self, filter, doc, upsert=False):
        self._assert_open()
        if len(filter) == 1 and '_id' in filter:
            self[filter['_id']] = doc
        else:
            for _id in self._find(filter):
                self[_id] = doc
                break

    def dump(self, file=sys.stdout):
        self._assert_open()
        for doc in self._docs.values():
            file.write(json.dumps(doc) + '\n')

    @classmethod
    def _open(cls, file):
        docs = (json.loads(line) for line in file)
        collection = cls(docs=docs)
        collection._file = file
        return collection

    @classmethod
    def open(cls, filename, mode='a+'):
        logger.debug("Open collection '{}'.".format(filename))
        if filename == ':memory:':
            file = io.StringIO()
        else:
            file = open(filename, mode)
            file.seek(0)
        return cls._open(file)

    def flush(self):
        self._assert_open()
        if self._dirty:
            if self._file is None:
                logger.debug("Flushed collection.")
            else:
                logger.debug("Flush collection to file '{}'.".format(self._file))
                self._file.truncate()
                self.dump(self._file)
                self._file.flush()
            self._dirty = False
        else:
            logger.debug("Flushed collection (no changes).")

    def close(self):
        if self._file is not None:
            self.flush()
            self._file.close()
            self._indeces.clear()
            self._docs = None
            self._file = None

    def __enter__(self):
        return self

    def __exit__(self, t, v, tb):
        self.close()
