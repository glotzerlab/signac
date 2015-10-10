import subprocess
import logging
from os.path import expanduser

import pymongo

DEFAULT_HOST_CONFIG = {
    'url': 'localhost',
    'auth_mechanism': 'none'}


def get_subject_from_certificate(fn_certificate):
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

try:
    import ssl
except ImportError:
    SSL_SUPPORT = False
else:
    SSL_SUPPORT = True

logger = logging.getLogger(__name__)

PYMONGO_3 = pymongo.version_tuple[0] == 3

AUTH_NONE = 'none'
AUTH_SCRAM_SHA_1 = 'SCRAM-SHA-1'
AUTH_SSL = 'SSL'
AUTH_SSL_x509 = 'SSL-x509'

SUPPORTED_AUTH_MECHANISMS = [AUTH_NONE,
                             AUTH_SCRAM_SHA_1, AUTH_SSL, AUTH_SSL_x509]

if SSL_SUPPORT:
    SSL_CERT_REQS = {
        'none': ssl.CERT_NONE,
        'optional': ssl.CERT_OPTIONAL,
        'required': ssl.CERT_REQUIRED
    }


def with_ssl_support():
    if not SSL_SUPPORT:
        raise EnvironmentError(
            "Your python installation does not support SSL.")


def raise_unsupported_auth_mechanism(mechanism):
    msg = "Auth mechanism '{}' not supported."
    raise ValueError(msg.format(mechanism))


class DBClientConnector(object):

    def __init__(self, host_config):
        self._config = host_config
        self._client = None

    @property
    def client(self):
        if self._client is None:
            raise RuntimeError("Client not connected.")
        else:
            return self._client

    @property
    def host(self):
        return self._config['url']

    def _config_get(self, key, default=None):
        return self._config.get(key, default)
        # try:
        #    return self._config[self._prefix + key]
        # except KeyError:
        # return self._config.get(key, default)

    def _config_get_required(self, key):
        return self._config[key]
        #result = self._config_get(key)
        # if result is None:
        #    self._config[key]
        # else:
        #    return result

    def _connect_pymongo3(self, host):
        parameters = {
            'connectTimeoutMS': self._config_get('connect_timeout_ms'),
        }

        auth_mechanism = self._config_get('auth_mechanism')
        if auth_mechanism in (AUTH_NONE, AUTH_SCRAM_SHA_1):
            client = pymongo.MongoClient(
                host,
                ** parameters)
        elif auth_mechanism in (AUTH_SSL, AUTH_SSL_x509):
            with_ssl_support()
            client = pymongo.MongoClient(
                host,
                ssl=True,
                ssl_keyfile=expanduser(
                    self._config_get_required('ssl_keyfile')),
                ssl_certfile=expanduser(
                    self._config_get_required('ssl_certfile')),
                ssl_cert_reqs=SSL_CERT_REQS[
                    self._config_get('ssl_cert_reqs', 'required')],
                ssl_ca_certs=expanduser(
                    self._config_get_required('ssl_ca_certs')),
                ssl_match_hostname=self._config_get(
                    'ssl_match_hostname', True),
                ** parameters)
        else:
            raise_unsupported_auth_mechanism(auth_mechanism)
        self._client = client

    def _connect_pymongo2(self, host):
        parameters = {
            'connectTimeoutMS': self._config_get('connect_timeout_ms'),
        }

        auth_mechanism = self._config_get('auth_mechanism')
        if auth_mechanism in (AUTH_NONE, AUTH_SCRAM_SHA_1):
            client = pymongo.MongoClient(
                host,
                ** parameters)
        elif auth_mechanism in (AUTH_SSL, AUTH_SSL_x509):
            msg = "SSL authentication not supported for pymongo versions <= 3.x ."
            logger.critical(msg)
            raise_unsupported_auth_mechanism(auth_mechanism)
        else:
            raise_unsupported_auth_mechanism(auth_mechanism)
        self._client = client

    def connect(self, host=None):
        if host is None:
            host = self._config_get_required('url')
        logger.debug("Connecting to host '{host}'.".format(
            host=self._config_get_required('url')))

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
            logger.debug(msg.format(user=username, db=db_admin))
            db_admin.authenticate(
                username,
                self._config_get_required('password'),
                mechanism=AUTH_SCRAM_SHA_1)
        elif auth_mechanism in (AUTH_SSL, AUTH_SSL_x509):
            with_ssl_support()
            certificate_subject = get_subject_from_certificate(
                expanduser(self._config_get_required('ssl_certfile')))
            logger.debug("Authenticating: user={}".format(certificate_subject))
            db_external = self.client['$external']
            db_external.authenticate(
                certificate_subject, mechanism='MONGODB-X509')

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
