from collections import defaultdict
import logging

from ..common import six
if six.PY2:
    from collections import Mapping
else:
    from collections.abc import Mapping

logger = logging.getLogger(__name__)


def _flatten(container):
    for i in container:
        if isinstance(i, (list, tuple)):
            for j in _flatten(i):
                yield j
        else:
            yield i


def _traverse_tree(t, include=None):
    if include is False:
        return
    if isinstance(t, list):
        for i in t:
            yield from _traverse_tree(i, include)
    elif isinstance(t, Mapping):
        for k in t:
            if include is None or include is True:
                for i in _traverse_tree(t[k]):
                    yield k, i
            else:
                if not include.get(k, False):
                    continue
                for i in _traverse_tree(t[k], include.get(k)):
                    yield k, i
    else:
        yield t


def _build_index(docs, include=None):
    index = defaultdict(set)
    ids = set()
    if include is None:
        included = None
    else:
        included = dict()
        for branch in _traverse_tree(include):
            f = tuple(_flatten(branch))
            included[hash(f[:-1])] = f[-1]
    for doc in docs:
        ids.add(doc['_id'])
        for branch in _traverse_tree(doc, include=include):
            f = tuple(_flatten(branch))
            index[hash(f)].add(doc['_id'])
    return ids, index, included


class DocumentSearchEngine(object):

    def __init__(self, ids, index, included=None):
        self.ids = ids
        self.index = index
        self.included = included

    def valid_filter(self, filter):
        if self.included is None:
            return True
        else:
            for branch in _traverse_tree(filter):
                h = hash(tuple(_flatten(branch))[:-1])
                if not self.included.get(h, True):
                    return False
            else:
                return True

    def find(self, filter=None):
        if filter is None or not len(filter):
            yield from self.ids
        else:
            if not self.valid_filter(filter):
                raise ValueError(filter)
            result = None
            for branch in _traverse_tree(filter):
                m = self.index.get(hash(tuple(_flatten(branch))))
                if result is None:
                    result = m
                    continue
                if m is None:
                    return
                else:
                    result = result.intersection(m)
            if result is None:
                return
            else:
                yield from result

    @classmethod
    def build_index(cls, docs, include=None):
        logger.debug("Building index...")
        return cls(* _build_index(docs=docs, include=include))

    def __len__(self):
        return len(self.ids)
