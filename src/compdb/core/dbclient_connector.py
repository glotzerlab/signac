import logging
logger = logging.getLogger(__name__)

import pymongo
PYMONGO_3 = pymongo.version_tuple[0] == 3

try:
    import ssl
except ImportError:
    SSL_SUPPORT = False
else:
    SSL_SUPPORT = True

AUTH_NONE = 'none'
AUTH_SCRAM_SHA_1 = 'SCRAM-SHA-1'
AUTH_SSL = 'SSL'
AUTH_SSL_x509 = 'SSL-x509'

SUPPORTED_AUTH_MECHANISMS = [AUTH_NONE, AUTH_SCRAM_SHA_1, AUTH_SSL, AUTH_SSL_x509]

if SSL_SUPPORT:
    SSL_CERT_REQS = {
        'none': ssl.CERT_NONE,
        'optional': ssl.CERT_OPTIONAL,
        'required': ssl.CERT_REQUIRED
    }

def with_ssl_support():
    if not SSL_SUPPORT:
        raise EnvironmentError("Your python installation does not support SSL.")

def raise_unsupported_auth_mechanism(mechanism):
    msg = "Auth mechanism '{}' not supported. Supported mechanisms: {}."
    raise ValueError(msg.format(mechanism, SUPPORTED_AUTH_MECHANISMS))

class DBClientConnector(object):

    def __init__(self, config, prefix = 'database_'):
        self._config = config
        self._prefix = prefix
        self._client = None

    @property
    def client(self):
        if self._client is None:
            raise RuntimeError("Client not connected.")
        else:
            return self._client

    def _config_get(self, key, default = None):
        try:
            return self._config[self._prefix + key]
        except KeyError:
            return self._config.get(key, default)

    def _config_get_required(self, key):
        result = self._config_get(key)
        if result is None:
            self._config[key]
        else:
            return result

    def _connect_pymongo3(self, host):
        from pymongo import MongoClient
        parameters = {
            'connectTimeoutMS': self._config_get('connect_timeout_ms'),
        }

        auth_mechanism = self._config_get('auth_mechanism')
        if auth_mechanism in (AUTH_NONE, AUTH_SCRAM_SHA_1):
            client = MongoClient(
                host,
                ** parameters)
        elif auth_mechanism in (AUTH_SSL, AUTH_SSL_x509):
            with_ssl_support()
            from os.path import expanduser
            client = MongoClient(
                host, 
                ssl = True,
                ssl_keyfile = expanduser(self._config_get_required('ssl_keyfile')),
                ssl_certfile = expanduser(self._config_get_required('ssl_certfile')),
                ssl_cert_reqs = SSL_CERT_REQS[self._config_get('ssl_cert_reqs', 'required')],
                ssl_ca_certs = expanduser(self._config_get_required('ssl_ca_certs')),
                ssl_match_hostname = self._config_get('ssl_match_hostname', True),
                ** parameters)
        else:
            raise_unsupported_auth_mechanism(auth_mechanism)
        self._client = client

    def _connect_pymongo2(self, host):
        from pymongo import MongoClient
        parameters = {
            'connectTimeoutMS': self._config_get('connect_timeout_ms'),
        }

        auth_mechanism = self._config_get('auth_mechanism')
        if auth_mechanism in (AUTH_NONE, AUTH_SCRAM_SHA_1):
            client = MongoClient(
                host,
                ** parameters)
        elif auth_mechanism in (AUTH_SSL, AUTH_SSL_x509):
            msg = "SSL authentication not supported for pymongo versions <= 3.x ."
            logger.critical(msg)
            raise_unsupported_auth_mechanism(auth_mechanism)
        else:
            raise_unsupported_auth_mechanism(auth_mechanism)
        self._client = client

    def connect(self, host = None):
        msg = "Connecting with config '{}' and prefix '{}'."
        if host is None:
            host = self._config_get_required('host')
        logger.debug(msg.format(self._config, self._prefix))
        logger.debug("Connecting to host '{host}'.".format(host=self._config_get_required('host')))

        if PYMONGO_3:
            self._connect_pymongo3(host)
        else:
            self._connect_pymongo2(host)

    def authenticate(self):
        auth_mechanism = self._config_get('auth_mechanism')
        logger.debug("Authenticating: mechanism={}".format(auth_mechanism))
        if auth_mechanism == AUTH_SCRAM_SHA_1:
            db_admin = self.client['admin']
            username = self._config_get_required('username')
            msg = "Authenticating user '{user}' with database '{db}'."
            logger.debug(msg.format(user=username,db=db_admin))
            db_admin.authenticate(
                username,
                self._config_get_required('password'),
                mechanism = AUTH_SCRAM_SHA_1)
        elif auth_mechanism in (AUTH_SSL, AUTH_SSL_x509):
            with_ssl_support()
            from os.path import expanduser
            from ..core.utility import get_subject_from_certificate
            certificate_subject = get_subject_from_certificate(expanduser(self._config_get_required('ssl_certfile')))
            logger.debug("Authenticating: user={}".format(certificate_subject))
            db_external = self.client['$external']
            db_external.authenticate(certificate_subject, mechanism = 'MONGODB-X509')

    def logout(self):
        auth_mechanism = self._config_get_required('auth_mechanism')
        if auth_mechanism == AUTH_SCRAM_SHA_1:
            db_admin = self.client['admin']
            db_admin.logout()
        elif auth_mechanism in (AUTH_SSL, AUTH_SSL_x509):
            db_external = self.client['$external']
            db_external.logout()
        else:
            raise_unsupported_auth_mechanism(auth_mechanism)
