# Copyright (c) 2016 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
import collections
import logging

import pymongo
from PySide import QtCore
from PySide.QtCore import Qt

logger = logging.getLogger(__name__)


class TreeItem(object):

    def __init__(self, data, parent):
        self.data = data
        self.parent = parent

    @property
    def children(self):
        return []

    def rowCount(self):
        return len(self.children)

    def columnCount(self):
        return 1

    def get_index(self, item):
        return self.children.index(item)

    def row(self):
        return self.parent.get_index(self)


class BasicTreeModel(QtCore.QAbstractItemModel):

    def __init__(self, parent=None):
        super(BasicTreeModel, self).__init__(parent)

    @property
    def children(self):
        raise NotImplementedError()

    def index(self, row, column, parent=QtCore.QModelIndex()):
        if not self.hasIndex(row, column, parent):
            return QtCore.QModelIndex()
        if parent.isValid():
            parent_node = parent.internalPointer()
            return self.createIndex(row, column, parent_node.children[row])
        else:  # root nodes
            return self.createIndex(row, column, self.children[row])

    def parent(self, index=QtCore.QModelIndex()):
        if not index.isValid():
            return QtCore.QModelIndex()
        node = index.internalPointer()
        parent = node.parent

        if parent is None:
            return QtCore.QModelIndex()
        else:
            return self.createIndex(parent.row(), 0, parent)

    def data(self, index, role=QtCore.Qt.DisplayRole):
        if index.isValid():
            if role == QtCore.Qt.DisplayRole:
                if index.column() == 0:
                    return str(index.internalPointer().name())
                elif index.column() == 1:
                    v = index.internalPointer().value()
                    return '' if v is None else str(v)
                elif index.column() == 2:
                    t = index.internalPointer().type()
                    return '' if t is None else str(t)

    def columnCount(self, parent):
        if parent.isValid():
            return parent.internalPointer().columnCount()
        else:
            return 1

    def rowCount(self, parent=QtCore.QModelIndex()):
        if parent.isValid():
            return parent.internalPointer().rowCount()
        else:
            return len(self.children)


class ListItem(TreeItem):

    def name(self):
        return self.data

    def value(self):
        return None

    def type(self):
        return None

    def columnCount(self):
        return 3


class MappingTreeModel(object):

    def __init__(self, key, data, parent):
        self.parent = parent
        self.key = key
        self._value = None
        self.children = []

        if isinstance(data, collections.Mapping):
            self.children = [MappingTreeModel(
                key, value, self) for key, value in data.items()]
        elif isinstance(data, list):
            self.children = [ListItem(v, self) for v in data]
        else:
            self._value = data

    def name(self):
        return str(self.key)

    def value(self):
        return self._value

    def type(self):
        if self._value is not None:
            return type(self._value)

    def row(self):
        return self.parent.get_index(self)

    def get_index(self, item):
        return self.children.index(item)

    def rowCount(self):
        return len(self.children)

    def columnCount(self):
        return 3


class DocumentTreeElement(TreeItem):

    def __init__(self, row, doc, parent=None):
        super(DocumentTreeElement, self).__init__(doc, parent)
        self._row = row
        self._items = None

    @property
    def doc(self):
        return self.data

    def row(self):
        return self._row

    def columnsCount(self):
        return 3

    @property
    def children(self):
        if self._items is None:
            self._items = [MappingTreeModel(key, value, self)
                           for key, value in self.doc.items()]
        return self._items

    def name(self):
        return "({row}) {_id}".format(
            row=self.row(), _id=self.doc.get('_id', ''))

    def value(self):
        return None

    def type(self):
        return None


class DocumentTreeModel(BasicTreeModel):

    def __init__(self, document, parent=None):
        super(DocumentTreeModel, self).__init__(parent)
        self.root = DocumentTreeElement(0, document)

    @property
    def children(self):
        return [self.root]

    def columnCount(self, parent=QtCore.QModelIndex()):
        return 3

    def headerData(self, section, orientation, role):
        if (orientation == Qt.Horizontal and role == Qt.DisplayRole):
            if section == 0:
                return "Key"
            elif section == 1:
                return "Value"
            elif section == 2:
                return "Type"

    def row(self):
        return 0


class CursorTreeModel(BasicTreeModel):

    def __init__(self, cursor, doc_index, parent=None):
        super(CursorTreeModel, self).__init__(parent)
        self._cursor = cursor
        self._doc_index = doc_index
        self._index_modified = True
        self._docs = None

    @property
    def doc_index(self):
        return self._doc_index

    @doc_index.setter
    def doc_index(self, value):
        value = max(0, int(value[0])), max(0, int(value[1]))
        if value != self._doc_index:
            self.modelAboutToBeReset.emit()
            self._doc_index = value
            self._index_modified = True
            self.modelReset.emit()

    @property
    def docs(self):
        return self.children

    @property
    def children(self):
        if self._docs is None or self._index_modified:
            cursor = self._cursor.clone()
            inherent_limit = cursor.count(True)
            cursor.skip(self.doc_index[0])
            delta = self.doc_index[1] - self.doc_index[0]
            cursor.limit(min(inherent_limit, delta))
            self._docs = [DocumentTreeElement(i, doc)
                          for i, doc in enumerate(cursor)]
            self._index_modified = False
        return self._docs

    def columnCount(self, parent=QtCore.QModelIndex()):
        return 3

    def headerData(self, section, orientation, role):
        if (orientation == Qt.Horizontal and role == Qt.DisplayRole):
            if section == 0:
                return "Key"
            elif section == 1:
                return "Value"
            elif section == 2:
                return "Type"


class DBCollectionTreeItem(TreeItem):

    @property
    def collection(self):
        return self.data

    def name(self):
        return self.collection.name


class DBTreeItem(TreeItem):

    def __init__(self, db, parent=None):
        super(DBTreeItem, self).__init__(db, parent)
        self._collections = None

    @property
    def db(self):
        return self.data

    @property
    def children(self):
        if self._collections is None:
            self._collections = [DBCollectionTreeItem(
                self.db[c], self) for c in self.db.collection_names()]
        return self._collections

    def name(self):
        return self.db.name


class DBClientTreeItem(TreeItem):

    def __init__(self, row, connector, parent=None):
        super(DBClientTreeItem, self).__init__(connector, parent)
        self._databases = None
        self._row = row

    @property
    def connector(self):
        return self.data

    @property
    def children(self):
        if self._databases is None:
            self.connector.connect()
            self.connector.authenticate()
            client = self.connector.client
            try:
                self._databases = [DBTreeItem(client[n], self)
                                   for n in client.database_names()]
            except pymongo.errors.OperationFailure:
                self._databases = [DBTreeItem(
                    client[self.connector.config['db_auth']], self)]
        return self._databases

    def name(self):
        return self.connector.host

    def row(self):
        return self._row

    def reload_databases(self):
        self._databases = None


class DBTreeModel(BasicTreeModel):

    def __init__(self, parent=None):
        super(DBTreeModel, self).__init__(parent)
        self.connectors = list()

    def add_connector(self, connector):
        i = len(self.connectors)
        parent = QtCore.QModelIndex()
        self.beginInsertRows(parent, i, i)
        self.connectors.append(DBClientTreeItem(i, connector))
        self.endInsertRows()

    def remove_connector(self, index):
        if index.isValid():
            while index.parent() != QtCore.QModelIndex():
                index = index.parent()
            self.beginRemoveRows(index.parent(), index.row(), index.row())
            self.connectors.pop(index.row())
            self.endRemoveRows()

    @property
    def children(self):
        return self.connectors

    def flags(self, index):
        return QtCore.Qt.ItemIsSelectable | QtCore.Qt.ItemIsEnabled

    def headerData(self, section, orientation, role):
        if (orientation == Qt.Horizontal and role == Qt.DisplayRole):
            return "Hosts"

    def reload_all(self):
        self.modelAboutToBeReset.emit()
        connectors = [item.connector for item in self.connectors]
        self.connectors = list()
        for connector in connectors:
            self.add_connector(connector)
