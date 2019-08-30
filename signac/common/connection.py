# Copyright (c) 2017 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
import subprocess
import logging
from os.path import expanduser
from deprecation import deprecated

import pymongo

from .errors import ConfigError


PYMONGO_2 = pymongo.version_tuple[0] == 2

logger = logging.getLogger(__name__)

AUTH_NONE = 'none'
AUTH_SCRAM_SHA_1 = 'SCRAM-SHA-1'
AUTH_SSL = 'SSL'
AUTH_SSL_x509 = 'SSL-x509'

# this is here because of issues importing the same variable in
# signac/__init__.py from the top level namespace
__version__ = '1.2.0'

@deprecated(deprecated_in="1.3", removed_in="2.0", current_version=__version__,
            details="The connection module is deprecated.")
def get_subject_from_certificate(fn_certificate):  # pragma no cover
    try:
        cert_txt = subprocess.check_output(
            ['openssl', 'x509', '-in', fn_certificate,
             '-inform', 'PEM', '-subject', '-nameopt', 'RFC2253']).decode()
    except subprocess.CalledProcessError:
        msg = "Unable to retrieve subject from certificate '{}'."
        raise RuntimeError(msg.format(fn_certificate))
    else:
        lines = cert_txt.split('\n')
        assert lines[0].startswith('subject=')
        return lines[0][len('subject='):].strip()


@deprecated(deprecated_ino="1.3", removed_in="2.0", current_version=__version__,
            details="The connection module is deprecated.")
def raise_unsupported_auth_mechanism(mechanism):
    msg = "Auth mechanism '{}' not supported."
    raise ValueError(msg.format(mechanism))


@deprecated(deprecated_ino="1.3", removed_in="2.0", current_version=__version__,
            details="The connection module is deprecated.")
class DBClientConnector(object):

    def __init__(self, host_config, **kwargs):
        self._config = host_config
        self._client = None
        self._kwargs = kwargs

    @deprecated(deprecated_ino="1.3", removed_in="2.0", current_version=__version__,
                details="The connection module is deprecated.")
    @property
    def client(self):
        if self._client is None:
            raise RuntimeError("Client not connected.")
        else:
            return self._client

    @deprecated(deprecated_ino="1.3", removed_in="2.0", current_version=__version__,
                details="The connection module is deprecated.")
    @property
    def host(self):
        return self._config['url']

    @deprecated(deprecated_ino="1.3", removed_in="2.0", current_version=__version__,
                details="The connection module is deprecated.")
    @property
    def config(self):
        return dict(self._config)

    @deprecated(deprecated_ino="1.3", removed_in="2.0", current_version=__version__,
                details="The connection module is deprecated.")
    def _config_get(self, key, default=None):
        return self._config.get(key, default)

    @deprecated(deprecated_ino="1.3", removed_in="2.0", current_version=__version__,
                details="The connection module is deprecated.")
    def _config_get_required(self, key):
        try:
            return self._config[key]
        except KeyError as e:
            raise ConfigError("Missing required key '{}'.".format(e))

    @deprecated(deprecated_ino="1.3", removed_in="2.0", current_version=__version__,
                details="The connection module is deprecated.")
    def _connect_pymongo3(self, host):
        logger.debug("Connecting with pymongo3.")
        forwarded_parameters = (
            'socketTimeoutMS', 'connectTimeoutMS', 'serverSelectionTimeoutMS',
            'w', 'wtimeout', 'replicaSet')
        parameters = self._kwargs
        for parameter in forwarded_parameters:
            if parameter in self._config:
                parameters[parameter] = self._config_get(parameter)

        auth_mechanism = self._config_get('auth_mechanism', AUTH_NONE)
        if auth_mechanism in (AUTH_NONE, AUTH_SCRAM_SHA_1):
            client = pymongo.MongoClient(
                host,
                read_preference=getattr(
                    pymongo.read_preferences.ReadPreference,
                    self._config_get('read_preference', 'PRIMARY')),
                ** parameters)
        else:
            raise_unsupported_auth_mechanism(auth_mechanism)
        self._client = client

    @deprecated(deprecated_ino="1.3", removed_in="2.0", current_version=__version__,
                details="The connection module is deprecated.")
    def connect(self, host=None):
        if host is None:
            host = self._config_get_required('url')
        logger.debug("Connecting to host '{host}'.".format(
            host=self._config_get_required('url')))

        if PYMONGO_2:
            raise RuntimeError("pymongo version 2.x is no longer supported.")
        else:
            self._connect_pymongo3(host)

    @deprecated(deprecated_ino="1.3", removed_in="2.0", current_version=__version__,
                details="The connection module is deprecated.")
    def authenticate(self):
        auth_mechanism = self._config_get('auth_mechanism', AUTH_NONE)
        logger.debug("Authenticating: mechanism={}".format(auth_mechanism))
        if auth_mechanism == AUTH_SCRAM_SHA_1:
            db_auth = self.client[self._config.get('db_auth', 'admin')]
            username = self._config_get_required('username')
            msg = "Authenticating user '{user}' with database '{db}'."
            logger.debug(msg.format(user=username, db=db_auth))
            db_auth.authenticate(
                username,
                self._config_get_required('password'),
                mechanism=AUTH_SCRAM_SHA_1)
        elif auth_mechanism in (AUTH_SSL, AUTH_SSL_x509):  # pragma no cover
            certificate_subject = get_subject_from_certificate(
                expanduser(self._config_get_required('ssl_certfile')))
            logger.debug("Authenticating: user={}".format(certificate_subject))
            db_external = self.client['$external']
            db_external.authenticate(
                certificate_subject, mechanism='MONGODB-X509')
        elif auth_mechanism == AUTH_NONE:
            pass
        else:
            raise_unsupported_auth_mechanism(auth_mechanism)

    @deprecated(deprecated_ino="1.3", removed_in="2.0", current_version=__version__,
                details="The connection module is deprecated.")
    def logout(self):
        auth_mechanism = self._config_get('auth_mechanism', AUTH_NONE)
        if auth_mechanism == AUTH_SCRAM_SHA_1:
            db_auth = self.client['admin']
            db_auth.logout()
        elif auth_mechanism in (AUTH_SSL, AUTH_SSL_x509):  # pragma no cover
            db_external = self.client['$external']
            db_external.logout()
        elif auth_mechanism == AUTH_NONE:
            pass
        else:
            raise_unsupported_auth_mechanism(auth_mechanism)
