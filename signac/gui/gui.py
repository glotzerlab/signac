#!/usr/bin/env python3
# Copyright (c) 2016 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
"""Light-weight GUI client as part of the signac framework.

The GUI can be used to view JSON files and access MongoDB
databases."""

import sys
import logging
import argparse
import json
import itertools

from PySide import QtCore, QtGui
from PySide.QtCore import Qt

from .models import CursorTreeModel, DocumentTreeModel,\
    DBTreeModel, DBCollectionTreeItem
from ..common.connection import DBClientConnector
from ..common.config import FN_CONFIG, read_config_file, load_config

logger = logging.getLogger(__name__)

DOC_INDEX_INC = 50
APPLICATION_NAME = 'signac-gui'
KEY_CONFIG_GUI = 'gui'
KEY_CONFIG_HOSTS = 'hosts'
ENCODING = 'utf-8'
KEY_SEQUENCE_QUIT = QtGui.QKeySequence(Qt.ControlModifier + Qt.Key_Q)
HIGH_NUM_DOCS_WARNING_THR = 1000

ABOUT_MSG = """
signac GUI

This basic JSON/MongoDB GUI Client is part of the signac framework.

Website: https://bitbucket.org/glotzer/signac
Author: Carl Simon Adorf, csadorf@umich.edu

Copyright (c) 2016 The Regents of the University of Michigan All rights reserved.

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE."""


def set_bg_color(w, color):
    return
    p = w.palette()
    p.setColor(w.backgroundRole(), color)
    w.setAutoFillBackground(True)
    w.setPalette(p)


def check_query(query):
    assert ';' not in query
    assert '\n' not in query
    assert 'import' not in query
    assert 'find(' in query
    tokens = query.split('.')
    assert tokens[0] == 'db'
#
#    tokens = query.split('(')
#    for legal_key in ('find(',):
#        if tokens[1].startswith(legal_key):
#            break
#    else:
#        assert False
#    assert tokens[-1].endswith(')')


def get_config():
    return read_config_file(FN_CONFIG)


def get_hosts_config(config=None):
    if config is None:
        config = get_config()
    return config.setdefault(KEY_CONFIG_HOSTS, dict())


def show_message(category, msg):
    msg_box = QtGui.QMessageBox(category, APPLICATION_NAME, msg)
    msg_box.exec_()


def show_error(msg):
    show_message(QtGui.QMessageBox.Critical, msg)


def show_warning(msg):
    show_message(QtGui.QMessageBox.Warning, msg)


class HostConnectionThread(QtCore.QThread):

    def run(self):
        try:
            self.parent().connector.connect()
            self.parent().connector.authenticate()
        except Exception as error:
            self.parent().connection_error = error
        else:
            self.parent().connection_error = None


class DocumentView(QtGui.QTreeView):

    def __init__(self, parent=None):
        super(DocumentView, self).__init__(parent)
        self.setSelectionMode(QtGui.QAbstractItemView.SingleSelection)
        self.setSelectionBehavior(QtGui.QAbstractItemView.SelectItems)


class FileSelectorEdit(QtGui.QWidget):

    def __init__(self, filename='', parent=None):
        super(FileSelectorEdit, self).__init__(parent)
        self.dialog = QtGui.QFileDialog(self, filename)
        self.dialog.setModal(True)
        self.edit = QtGui.QLineEdit()
        self.button = QtGui.QPushButton('...')
        main_layout = QtGui.QHBoxLayout()
        main_layout.addWidget(self.edit)
        main_layout.addWidget(self.button)
        self.setLayout(main_layout)

        self.edit.textChanged.connect(self.dialog.selectFile)
        self.dialog.fileSelected.connect(self.edit.setText)
        self.button.clicked.connect(self.dialog.show)

    def filename(self):
        return self.edit.text()

    def set_filename(self, filename):
        self.edit.setText(filename)


class FileSelectorBox(QtGui.QWidget):

    def __init__(self, text, parent=None):
        super(FileSelectorBox, self).__init__(parent)
        self.label = QtGui.QLabel(text)
        self.file_selector_edit = FileSelectorEdit(text, self)
        box_layout = QtGui.QVBoxLayout()
        box_layout.setContentsMargins(0, 0, 0, 0)
        box_layout.addWidget(self.label)
        box_layout.addWidget(self.file_selector_edit)
        self.setLayout(box_layout)

    def filename(self):
        return self.file_selector_edit.filename()

    def set_filename(self, filename):
        self.file_selector_edit.set_filename(filename)


class HostTab(QtGui.QWidget):

    def __init__(self, parent=None):
        super(HostTab, self).__init__(parent)
        host_url_label = QtGui.QLabel("Host")
        self.host_url_edit = QtGui.QLineEdit()
        main_layout = QtGui.QFormLayout()
        main_layout.addWidget(host_url_label)
        main_layout.addWidget(self.host_url_edit)
        self.setLayout(main_layout)


class AuthenticationTab(QtGui.QWidget):

    def __init__(self, parent=None):
        super(AuthenticationTab, self).__init__(parent)
        set_bg_color(self, Qt.red)

        self.none_button = QtGui.QRadioButton("None")
        self.scram_button = QtGui.QRadioButton("SCRAM-SHA-1")
        self.scram_group = QtGui.QGroupBox()
        scram_layout = QtGui.QVBoxLayout()
        scram_layout.setContentsMargins(0, 0, 0, 0)
        username_label = QtGui.QLabel("Username")
        self.username_edit = QtGui.QLineEdit()
        password_label = QtGui.QLabel("Password")
        self.password_edit = QtGui.QLineEdit()
        self.password_edit.setEchoMode(QtGui.QLineEdit.Password)
        scram_layout.addWidget(username_label)
        scram_layout.addWidget(self.username_edit)
        scram_layout.addWidget(password_label)
        scram_layout.addWidget(self.password_edit)
        self.scram_group.setLayout(scram_layout)
        self.ssl_button = QtGui.QRadioButton("SSL-x509")
        self.ssl_ca_certs_edit = FileSelectorBox('CA Certificates', self)
        set_bg_color(self.ssl_ca_certs_edit, Qt.green)
        self.ssl_certfile_edit = FileSelectorBox('Certificate')
        self.ssl_keyfile_edit = FileSelectorBox('Key file')
        ssl_layout = QtGui.QVBoxLayout()
        ssl_layout.setContentsMargins(0, 0, 0, 0)
        ssl_layout.addWidget(self.ssl_ca_certs_edit)
        ssl_layout.addWidget(self.ssl_certfile_edit)
        ssl_layout.addWidget(self.ssl_keyfile_edit)
        self.ssl_group = QtGui.QGroupBox()
        set_bg_color(self.ssl_group, Qt.yellow)
        self.ssl_group.setLayout(ssl_layout)
        db_auth_label = QtGui.QLabel("Authentication database")
        self.db_auth_edit = QtGui.QLineEdit('admin')

        main_layout = QtGui.QFormLayout()
        main_layout.addWidget(self.none_button)
        main_layout.addWidget(self.scram_button)
        main_layout.addWidget(self.scram_group)
        main_layout.addWidget(self.ssl_button)
        main_layout.addWidget(self.ssl_group)
        main_layout.addWidget(db_auth_label)
        main_layout.addWidget(self.db_auth_edit)
        self.setLayout(main_layout)
        self.none_button.toggled.connect(self.sync)
        self.scram_button.toggled.connect(self.sync)

    def sync(self, on=False):
        if self.none_button.isChecked():
            self.scram_group.setEnabled(False)
            self.ssl_group.setEnabled(False)
        elif self.scram_button.isChecked():
            self.scram_group.setEnabled(True)
            self.ssl_group.setEnabled(False)
        elif self.ssl_button.isChecked():
            self.scram_group.setEnabled(False)
            self.ssl_group.setEnabled(True)

    @property
    def auth_mechanism(self):
        if self.none_button.isChecked():
            return 'none'
        elif self.scram_button.isChecked():
            return 'SCRAM-SHA-1'
        elif self.ssl_button.isChecked():
            return 'SSL-x509'

    @auth_mechanism.setter
    def auth_mechanism(self, value):
        if value == 'none':
            self.none_button.setChecked(True)
        elif value == 'SCRAM-SHA-1':
            self.scram_button.setChecked(True)
        elif value == 'SSL-x509':
            self.ssl_button.setChecked(True)
        else:
            raise RuntimeError(
                "Auth mechanism '{}' not supported.".format(value))
        self.sync()


class HostList(QtGui.QTableWidget):

    def __init__(self, parent=None):
        super(HostList, self).__init__(parent)
        self.setSelectionMode(QtGui.QAbstractItemView.SingleSelection)
        self.setSelectionBehavior(QtGui.QAbstractItemView.SelectRows)
        self.setColumnCount(2)
        self.setHorizontalHeaderItem(0, QtGui.QTableWidgetItem("Name"))
        self.setHorizontalHeaderItem(1, QtGui.QTableWidgetItem("Url"))
        self.load_config()

    def load_config(self):
        hosts_config = get_hosts_config()
        self.setRowCount(len(hosts_config))
        for i, (key, host_config) in enumerate(hosts_config.items()):
            item = QtGui.QTableWidgetItem(key)
            item.setFlags(item.flags() ^ Qt.ItemIsEditable)
            self.setItem(i, 0, item)
            item = QtGui.QTableWidgetItem(_get_url(host_config.get('url', '')))
            item.setFlags(item.flags() ^ Qt.ItemIsEditable)
            self.setItem(i, 1, item)

    def selected_host(self):
        selected_indeces = self.selectedIndexes()
        if len(selected_indeces):
            index = self.selectedIndexes()[0]
            return self.item(index.row(), 0).text()
        else:
            return None

    def select_host(self, hostname):
        items = self.findItems(hostname, Qt.MatchExactly)
        if len(items):
            self.setCurrentItem(items[0])


class HostsDialog(QtGui.QDialog):
    attempt_connection = QtCore.Signal()
    host_connected = QtCore.Signal()
    connection_failed = QtCore.Signal()

    def __init__(self, parent=None):
        super(HostsDialog, self).__init__(parent)
        self.setModal(True)
        self.setWindowTitle("Hosts")
        self.connector = None
        self.connection_error = None
        self.connect_host_thread = HostConnectionThread(self)
        self.setupUI()

    def setupUI(self):
        hosts_layout = QtGui.QVBoxLayout()

        new_button = QtGui.QPushButton("New")
        self.edit_button = QtGui.QPushButton("Edit")
        self.edit_button.setEnabled(False)
        self.remove_button = QtGui.QPushButton("Remove")
        self.remove_button.setEnabled(False)
        new_edit_remove_layout = QtGui.QHBoxLayout()
        new_edit_remove_layout.addWidget(new_button)
        new_edit_remove_layout.addWidget(self.edit_button)
        new_edit_remove_layout.addWidget(self.remove_button)
        new_edit_remove_box = QtGui.QGroupBox()
        new_edit_remove_box.setLayout(new_edit_remove_layout)

        self.hosts_list = HostList()
        hosts_layout.addWidget(self.hosts_list)
        hosts_layout.addWidget(new_edit_remove_box)
        hosts_group = QtGui.QGroupBox("Hosts")
        hosts_group.setLayout(hosts_layout)

        button_box = QtGui.QDialogButtonBox(
            QtGui.QDialogButtonBox.Ok | QtGui.QDialogButtonBox.Cancel)
        button_box.accepted.connect(self.accept)
        button_box.rejected.connect(self.reject)

        main_layout = QtGui.QFormLayout()
        main_layout.addWidget(hosts_group)
        main_layout.addWidget(button_box)

        self.setLayout(main_layout)

        new_button.clicked.connect(self.new_host)
        self.edit_button.clicked.connect(self.edit_host)
        self.remove_button.clicked.connect(self.remove_host)
        # self.hosts_list.itemActivated.connect(self.item_activated)
        self.hosts_list.itemDoubleClicked.connect(self.item_activated)
        self.hosts_list.itemSelectionChanged.connect(
            self.host_selection_changed)

        self.connect_host_thread.started.connect(self.attempt_connection)
        self.connect_host_thread.finished.connect(self.connect_attempt_finished)

    def all_hostnames(self):
        return get_hosts_config().keys()

    def item_activated(self, item):
        self.accept()
        # self.edit_host()

    def host_selection_changed(self):
        have_selection = len(self.hosts_list.selectedIndexes())
        self.edit_button.setEnabled(have_selection)
        self.remove_button.setEnabled(have_selection)

    def load_config(self):
        self.hosts_list.load_config()

    def new_host(self):
        hostnames = set(self.all_hostnames())
        hostname = ('New Host{}'.format(i) for i in itertools.count(1))
        for new_hostname in hostname:
            if new_hostname not in hostnames:
                break
        connect_dialog = ConnectDialog(new_hostname,  self)
        connect_dialog.show()
        connect_dialog.change_name()

    def edit_host(self):
        name = self.hosts_list.selected_host()
        connect_dialog = ConnectDialog(name, self)
        connect_dialog.show()

    def remove_host(self):
        name = self.hosts_list.selected_host()
        msg = "Are you sure you want to remove host '{}'?"
        answer = QtGui.QMessageBox.question(
            self,
            APPLICATION_NAME,
            msg.format(name),
            QtGui.QMessageBox.Yes | QtGui.QMessageBox.No) == \
            QtGui.QMessageBox.Yes
        if answer:
            config = get_config()
            hosts_config = get_hosts_config(config)
            try:
                del hosts_config[name]
            except KeyError:
                pass
            else:
                config.write()
        self.load_config()

    def accept(self):
        if self.connect_host_thread.isRunning():
            show_warning("Already connecting!")
            return False
        selected_host = self.hosts_list.selected_host()
        if selected_host is not None:
            host_config = get_hosts_config()[selected_host]
            host_config['serverSelectionTimeoutMS'] = 3000
            self.connector = DBClientConnector(host_config)
            self.connect_host_thread.start()
            return super(HostsDialog, self).accept()
        else:
            show_warning("No host selected.")
            return False

    def connect_attempt_finished(self):
        if self.connection_error is None:
            self.host_connected.emit()
        else:
            error = self.connection_error
            self.connection_failed.emit()
            self.parent().set_status("Error.", 5000)
            msg_box = QtGui.QMessageBox(
                QtGui.QMessageBox.Warning,
                "Connection Error",
                "{}: '{}'".format(type(error), error))
            msg_box.exec_()


class HostnameValidator(QtGui.QValidator):

    def __init__(self, name, parent=None):
        super(HostnameValidator, self).__init__(parent)
        self.name = name

    def validate(self, input_, pos):
        if input_ == self.name:
            return HostnameValidator.Acceptable
        else:
            hostnames = set(get_hosts_config().keys())
            if input_ in hostnames and not input_ == self.name:
                return HostnameValidator.Intermediate
            else:
                return HostnameValidator.Acceptable


class ConnectDialog(QtGui.QDialog):

    class RenameDialog(QtGui.QDialog):

        def __init__(self, parent=None):
            super(ConnectDialog.RenameDialog, self).__init__(parent)
            self.setModal(True)
            main_layout = QtGui.QFormLayout()
            main_layout.addWidget(QtGui.QLabel('New name'))
            self.edit = QtGui.QLineEdit()
            hostname = self.parent().hostname()
            self.edit.setValidator(HostnameValidator(hostname))
            self.edit.setText(hostname)
            button_box = QtGui.QDialogButtonBox(
                QtGui.QDialogButtonBox.Ok | QtGui.QDialogButtonBox.Cancel)
            button_box.accepted.connect(self.accept)
            button_box.rejected.connect(self.reject)

            main_layout.addWidget(self.edit)
            main_layout.addWidget(button_box)
            self.setLayout(main_layout)
            self.edit.selectAll()

        def accept(self):
            if not self.edit.hasAcceptableInput():
                show_warning("Host name already in use.")
                return
            else:
                self.parent().name_edit.setText(self.edit.text())
            super(ConnectDialog.RenameDialog, self).accept()

    def __init__(self, hostname, parent=None):
        super(ConnectDialog, self).__init__(parent)
        self.setupUI()
        self.name_edit.setValidator(HostnameValidator(hostname))
        self.name_edit.setText(hostname)
        self._hostname = hostname
        self.load_config()

    def setupUI(self):
        self.setModal(True)
        self.setWindowTitle("Connect...")

        name_label = QtGui.QLabel("Connection name")
        self.name_edit = QtGui.QLineEdit()
        self.name_edit.setReadOnly(True)
        self.name_edit_change = QtGui.QPushButton("Rename...")
        self.name_edit_change.clicked.connect(self.change_name)
        name_grid = QtGui.QGridLayout()
        name_grid.addWidget(name_label, 0, 0, 1, 2)
        name_grid.addWidget(self.name_edit, 1, 0, 1, 1)
        name_grid.addWidget(self.name_edit_change, 1, 1, 1, 1)
        name_box = QtGui.QGroupBox()
        name_box.setLayout(name_grid)

        self.tabs = QtGui.QTabWidget(self)
        self.host_tab_index = self.tabs.addTab(HostTab(), '&Host')
        self.auth_tab_index = self.tabs.addTab(
            AuthenticationTab(), '&Authentication')
        button_box = QtGui.QDialogButtonBox(
            QtGui.QDialogButtonBox.Ok | QtGui.QDialogButtonBox.Cancel)
        button_box.accepted.connect(self.accept)
        button_box.rejected.connect(self.reject)
        main_layout = QtGui.QVBoxLayout()
        main_layout.addWidget(name_box)
        main_layout.addWidget(self.tabs)
        main_layout.addWidget(button_box)
        self.setLayout(main_layout)

    def hostname(self):
        return self.name_edit.text()

    def change_name(self):
        new_name_dialog = ConnectDialog.RenameDialog(self)
        new_name_dialog.show()

    def sizeHint(self):
        return QtCore.QSize(400, 400)

    def load_config(self):
        hosts_config = get_hosts_config()
        host_config = hosts_config.get(self.name_edit.text(), dict())
        host_tab = self.tabs.widget(self.host_tab_index)
        auth_tab = self.tabs.widget(self.auth_tab_index)
        host_tab.host_url_edit.setText(_get_url(host_config.get('url', 'mongodb://localhost')))
        auth_tab.username_edit.setText(host_config.get('username', ''))
        auth_tab.password_edit.setText(host_config.get('password', ''))
        auth_tab.auth_mechanism = host_config.get('auth_mechanism', 'none')
        auth_tab.ssl_ca_certs_edit.set_filename(
            host_config.get('ssl_ca_certs', ''))
        auth_tab.ssl_certfile_edit.set_filename(
            host_config.get('ssl_certfile', ''))
        auth_tab.ssl_keyfile_edit.set_filename(
            host_config.get('ssl_keyfile', ''))
        auth_tab.db_auth_edit.setText(host_config.get('db_auth', 'admin'))

    def update_config(self):
        assert self.name_edit.hasAcceptableInput()
        hostname = self._hostname
        host_tab = self.tabs.widget(self.host_tab_index)
        auth_tab = self.tabs.widget(self.auth_tab_index)
        config = get_config()
        hosts_config = get_hosts_config(config)
        host_config = hosts_config.setdefault(hostname, dict())
        host_config['url'] = _set_url(host_tab.host_url_edit.text())
        host_config['auth_mechanism'] = auth_tab.auth_mechanism
        host_config['username'] = auth_tab.username_edit.text()
        host_config['password'] = auth_tab.password_edit.text()
        host_config['ssl_ca_certs'] = auth_tab.ssl_ca_certs_edit.filename()
        host_config['ssl_certfile'] = auth_tab.ssl_certfile_edit.filename()
        host_config['ssl_keyfile'] = auth_tab.ssl_keyfile_edit.filename()
        host_config['db_auth'] = auth_tab.db_auth_edit.text()
        if self.hostname() != hostname:  # rename event
            hosts_config[self.hostname()] = host_config
            del hosts_config[hostname]
            self._hostname = self.hostname()
        config.write()

    def accept(self):
        self.update_config()
        self.parent().load_config()
        self.parent().hosts_list.select_host(self.hostname())
        super(ConnectDialog, self).accept()


class FileDialog(QtGui.QFileDialog):
    pass


class MainWindow(QtGui.QMainWindow):

    def __init__(self, parent=None):
        super(MainWindow, self).__init__(parent)
        self.setWindowTitle(APPLICATION_NAME)
        self.config = None
        self.load_config()
        self.setupUI()
        self.hosts_dialog.show()
        self.hosts_dialog.setFocus()

    def show_about(self):
        QtGui.QMessageBox.about(self, APPLICATION_NAME, ABOUT_MSG)

    def setupUI(self):
        self.menuBar().show()
        self.main_view = MainView(self)
        self.setCentralWidget(self.main_view)

        self.hosts_dialog = HostsDialog(self)
        fileMenu = self.menuBar().addMenu('&File')
        fileMenu.addAction(
            '&Connect...', self.hosts_dialog.show, QtGui.QKeySequence.New)
        self.file_dialog = FileDialog(self)
        self.file_dialog.setDefaultSuffix('.json')
        self.file_dialog.setFilter('*.json')
        fileMenu.addAction('&Open...', self.file_dialog.show,
                           QtGui.QKeySequence.Open)
        self.file_dialog.fileSelected.connect(self.open_file)
        fileMenu.addAction('&Quit..', self.close, KEY_SEQUENCE_QUIT)

        dbMenu = self.menuBar().addMenu('&Database')
        dbMenu.addAction('&Reload', self.reload_collections).setEnabled(False)  # currently defunct
        dbMenu.addAction('&Close', self.close_connection)
        dbMenu.addSeparator()
        dbMenu.addAction('Reload &all', self.reload_all_collections)

        helpMenu = self.menuBar().addMenu('&Help')
        about_action = helpMenu.addAction('&About signac')
        about_action.triggered.connect(self.show_about)
        self.qt_about_action = helpMenu.addAction('About &Qt')

        gui = self.config.get(KEY_CONFIG_GUI)
        if gui is not None:
            geometry = QtCore.QByteArray.fromBase64(gui.get('geometry'))
            self.restoreGeometry(geometry)
            windowState = QtCore.QByteArray.fromBase64(gui.get('windowState'))
            self.restoreGeometry(windowState)

        self.hosts_dialog.host_connected.connect(self.host_connected)
        self.hosts_dialog.connection_failed.connect(self.host_connection_failed)
        self.hosts_dialog.attempt_connection.connect(self.attempt_connection)

    def sizeHint(self):
        return QtCore.QSize(1024, 768)

    def set_status(self, msg, timeout=0):
        self.statusBar().showMessage(msg, timeout)

    def load_config(self):
        self.set_status("Loading config...")
        self.config = load_config()
        self.set_status("Done.", 3000)

    def write_config(self):
        tmp = get_config()
        gui = tmp.setdefault(KEY_CONFIG_GUI, dict())
        gui['geometry'] = self.saveGeometry().toBase64()
        gui['windowState'] = self.saveState().toBase64()
        tmp.write()

    def attempt_connection(self):
        self.set_status("Connecting...")

    def host_connected(self):
        connector = self.hosts_dialog.connector
        self.main_view.db_tree_model.add_connector(connector)
        self.set_status(
            "Connected to '{}'.".format(connector.client),
            5000)

    def host_connection_failed(self):
        self.set_status("Connection attempt failed.", 5000)
        self.hosts_dialog.show()

    def reload_all_collections(self):
        self.main_view.db_tree_model.reload_all()

    def reload_collections(self):
        selection = self.main_view.db_tree_view.selectedIndexes()
        for s in selection:
            self.main_view.db_tree_model.reload_connector(s)

    def close_connection(self):
        selection = self.main_view.db_tree_view.selectedIndexes()
        for s in selection:
            self.main_view.db_tree_model.remove_connector(s)

    def open_file(self, fn):
        logger.info('open file({})'.format(fn))
        with open(fn, 'rb') as file:
            mapping = json.loads(file.read().decode())
        document_view = DocumentView()
        document_view.setWindowTitle(fn)
        document_view.setModel(DocumentTreeModel(mapping))
        self.main_view.mdi_area.addSubWindow(document_view)
        document_view.show()

    def open_quit_dialog(self):
        return QtGui.QMessageBox.question(
            self,
            APPLICATION_NAME, "Are you sure you want to quit?",
            QtGui.QMessageBox.No | QtGui.QMessageBox.Yes) == \
            QtGui.QMessageBox.Yes

    def closeEvent(self, event):
        if self.open_quit_dialog():
            self.write_config()
            event.accept()
        else:
            event.ignore()


class MdiArea(QtGui.QMdiArea):
    pass


class MainView(QtGui.QWidget):

    def __init__(self, parent=None):
        super(MainView, self).__init__(parent)
        self.db_tree_model = DBTreeModel()
        self.setupUI()
        self.setupLogic()
        self.query_views = list()

    def setupUI(self):
        main_layout = QtGui.QHBoxLayout()
        splitter = QtGui.QSplitter()
        self.db_tree_view = DBTreeView(self)
        self.db_tree_view.setModel(self.db_tree_model)
        self.mdi_area = MdiArea(self)
        splitter.addWidget(self.db_tree_view)
        splitter.addWidget(self.mdi_area)
        main_layout.addWidget(splitter)
        self.setLayout(main_layout)

    def setupLogic(self):
        self.db_tree_view.doubleClicked.connect(self.open_query)

    def set_status(self, msg, timeout=0):
        self.parent().set_status(msg=msg, timeout=timeout)

    def querying(self):
        self.set_status("Querying...")

    def query_done(self):
        self.set_status("Done.", 5000)

    def open_query(self, index):
        if index.isValid():
            node = index.internalPointer()
            if isinstance(node, DBCollectionTreeItem):
                query_view = QueryView(node.parent.db, self)
                query_view.setWindowTitle(node.parent.db.name)
                query_view.set_collection(node.collection)
                query_view.query_begin.connect(self.querying)
                query_view.query_done.connect(self.query_done)
                self.mdi_area.addSubWindow(query_view)
                query_view.show()
                query_view.execute_query()


class DBTreeView(QtGui.QTreeView):

    def __init__(self, parent=None):
        super(DBTreeView, self).__init__(parent)
        self.setHeaderHidden(True)
        self.setSizePolicy(QtGui.QSizePolicy.Minimum,
                           QtGui.QSizePolicy.Expanding)
        self.setSelectionMode(QtGui.QAbstractItemView.SingleSelection)
        self.setSelectionBehavior(QtGui.QAbstractItemView.SelectRows)

    def minimumSizeHint(self):
        return QtCore.QSize(250, 500)


class QueryThread(QtCore.QThread):
    query_result_available = QtCore.Signal()
    query_failed = QtCore.Signal()

    def __init__(self, parent):
        super(QueryThread, self).__init__(parent)
        self.db = None
        self.result_cursor = None
        self.error = None

    def run(self):
        query_cmd = self.parent().query_edit.text()
        try:
            self.result_cursor = eval(query_cmd, {'db': self.db})
        except Exception as error:
            self.error = error
            self.query_failed.emit()
        else:
            self.query_result_available.emit()


class QueryView(QtGui.QWidget):
    query_begin = QtCore.Signal()
    query_done = QtCore.Signal()

    def __init__(self, db, parent=None):
        super(QueryView, self).__init__(parent)
        self.db = db
        self.query_thread = QueryThread(self)
        self.setupUI()
        self.setupLogic()

    def setupUI(self):
        main_layout = QtGui.QVBoxLayout()
        self.setSizePolicy(QtGui.QSizePolicy.Expanding,
                           QtGui.QSizePolicy.Expanding)
        self.setLayout(main_layout)
        self.query_edit = QtGui.QLineEdit(self)
        self.tree_view = QtGui.QTreeView(self)
        self.tree_view.setSelectionBehavior(
            QtGui.QAbstractItemView.SelectItems)
        self.tree_view.setSelectionMode(
            QtGui.QAbstractItemView.SingleSelection)
        self.scroll_group = QtGui.QGroupBox(self)
        scroll_layout = QtGui.QHBoxLayout()
        scroll_layout.setAlignment(Qt.AlignRight)
        self.back_button = QtGui.QPushButton('<')
        self.forward_button = QtGui.QPushButton('>')
        self.index_start_edit = QtGui.QLineEdit('0')
        self.index_stop_edit = QtGui.QLineEdit(str(DOC_INDEX_INC))
        self.index_validator = QtGui.QIntValidator()
        self.index_validator.setBottom(0)
        self.index_start_edit.setValidator(self.index_validator)
        self.index_stop_edit.setValidator(self.index_validator)
        self.cursor_count_label = QtGui.QLabel("max. # records")
        self.cursor_count_edit = QtGui.QLineEdit()
        self.cursor_count_edit.setEnabled(False)
        scroll_layout.addWidget(self.back_button)
        scroll_layout.addWidget(self.index_start_edit)
        scroll_layout.addWidget(self.index_stop_edit)
        scroll_layout.addWidget(self.forward_button)
        scroll_layout.addWidget(self.cursor_count_label)
        scroll_layout.addWidget(self.cursor_count_edit)
        self.scroll_group.setLayout(scroll_layout)
        main_layout.addWidget(self.query_edit)
        main_layout.addWidget(self.tree_view)
        main_layout.addWidget(self.scroll_group)

    def setupLogic(self):
        self.query_edit.returnPressed.connect(self.execute_query)
        self.forward_button.clicked.connect(self.increase_doc_index)
        self.back_button.clicked.connect(self.decrease_doc_index)
        self.index_start_edit.returnPressed.connect(self.update_index)
        self.index_stop_edit.returnPressed.connect(self.update_index)
        self.query_thread.query_result_available.connect(self.execute_query_success)
        self.query_thread.query_failed.connect(self.execute_query_failed)

    def set_collection(self, collection):
        query = "db.{collection}.find()"
        self.query_edit.setText(query.format(collection=collection.name))

    def execute_query(self):
        assert not self.query_thread.isRunning()
        self.query_edit.setEnabled(False)
        query_cmd = self.query_edit.text().strip()
        try:
            check_query(query_cmd)
        except AssertionError:
            msg_box = QtGui.QMessageBox(
                QtGui.QMessageBox.Information,
                "Illformed query",
                query_cmd)
            msg_box.exec_()
            self.query_edit.setEnabled(True)
        else:
            self.query_thread.db = self.db
            self.query_thread.query_cmd = query_cmd
            self.query_thread.start()

    def enable(self):
        self.setEnabled(True)

    def disable(self):
        self.setEnabled(False)

    def execute_query_success(self):
        self.query_edit.setEnabled(True)
        result_cursor = self.query_thread.result_cursor
        self.cursor_count_edit.setText(str(result_cursor.count()))
        docs_model = CursorTreeModel(
            result_cursor, (0, DOC_INDEX_INC), self)
        docs_model.modelReset.connect(self.query_done)
        docs_model.modelReset.connect(self.enable)
        docs_model.modelAboutToBeReset.connect(self.disable)
        self.tree_view.setModel(docs_model)
        self.tree_view.setExpanded(docs_model.index(0, 0), True)
        self.tree_view.resizeColumnToContents(0)
        self.tree_view.resizeColumnToContents(1)
        self.tree_view.resizeColumnToContents(2)

    def execute_query_failed(self):
        self.query_edit.setEnabled(True)
        error = self.query_thread.error
        msg_box = QtGui.QMessageBox(
            QtGui.QMessageBox.Warning,
            "Error while executing Query",
            "'{}': {}".format(type(error), error))
        msg_box.exec_()

    def sizeHint(self):
        return QtCore.QSize(860, 640)

    @property
    def index_start(self):
        return max(0, int(self.index_start_edit.text()))

    @index_start.setter
    def index_start(self, value):
        self.index_start_edit.setText(str(max(0, int(value))))

    @property
    def index_stop(self):
        return max(0, int(self.index_stop_edit.text()))

    @index_stop.setter
    def index_stop(self, value):
        self.index_stop_edit.setText(str(max(1, int(value))))

    def increase_doc_index(self):
        delta = self.index_stop - self.index_start
        self.index_start += delta
        self.index_stop += delta
        self.update_index()

    def decrease_doc_index(self):
        if self.index_start <= 0:
            return
        delta = self.index_stop - self.index_start
        self.index_start -= delta
        self.index_stop -= delta
        self.update_index()

    def update_index(self):
        num_docs = self.index_stop - self.index_start
        if num_docs > HIGH_NUM_DOCS_WARNING_THR:
            msg = "The number of document you selected to show is very high. Do you want to reduce them?"
            answer = QtGui.QMessageBox.question(
                self, APPLICATION_NAME, msg,
                QtGui.QMessageBox.Yes | QtGui.QMessageBox.No) == \
                QtGui.QMessageBox.Yes
            if answer:
                self.index_stop = self.index_start + HIGH_NUM_DOCS_WARNING_THR
        self.query_begin.emit()
        self.tree_view.model().doc_index = self.index_start, self.index_stop


def _get_url(url):
    if isinstance(url, list):
        return ','.join(url)
    else:
        return url


def _set_url(url_str):
    return ','.join((s.strip() for s in url_str.split(',')))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--debug',
        action='store_true',
        help="Increase output verbosity.")
    args = parser.parse_args()
    if args.debug:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)
    app = QtGui.QApplication(sys.argv)
    main_window = MainWindow()
    main_window.qt_about_action.triggered.connect(app.aboutQt)
    main_window.show()
    sys.exit(app.exec_())

if __name__ == '__main__':
    sys.exit(main())
