# Copyright (c) 2016 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the MIT License.
import logging
import getpass

from .config import load_config
from .errors import ConfigError, AuthenticationError
from .connection import DBClientConnector
from .crypt import get_crypt_context
from . import six

logger = logging.getLogger(__name__)


def get_host_config(hostname=None, config=None):
    if config is None:
        config = load_config()
    if hostname is None:
        try:
            hostname = config['General']['default_host']
        except KeyError:
            try:
                hostname = config['hosts'].keys()[0]
            except (KeyError, IndexError):
                raise ConfigError("No hosts specified.")
    try:
        return config['hosts'][hostname]
    except KeyError:
        raise ConfigError("Host '{}' not configured.".format(hostname))


def get_current_password(hostcfg):
    pw = hostcfg.get('password')
    pwcfg = hostcfg.get('password_config')
    if pwcfg:
        logger.debug("Found password configuration: {}".format(pwcfg))
    if pw is None:
        pw = getpass.getpass("Enter password for {}@{}: ".format(
            hostcfg['username'], hostcfg['url']))
        if pwcfg:
            return get_crypt_context().encrypt(pw, **pwcfg)
        else:
            return pw
    else:
        return pw


def check_credentials(hostcfg):
    input_ = raw_input if six.PY2 else input  # noqa
    auth_m = hostcfg.get('auth_mechanism', 'none')
    if auth_m == 'SCRAM-SHA-1':
        if 'username' not in hostcfg:
            username = input("Username ({}): ".format(getpass.getuser()))
            if not username:
                username = getpass.getuser()
            hostcfg['username'] = username
        if 'password' not in hostcfg:
            hostcfg['password'] = get_current_password(hostcfg)
    return hostcfg


def get_connector(hostname=None, config=None, **kwargs):
    hostcfg = check_credentials(
        get_host_config(hostname=hostname, config=config))
    logger.debug("Connecting with host config: {}".format(hostcfg))
    return DBClientConnector(hostcfg, **kwargs)


def get_client(hostname=None, config=None, **kwargs):
    connector = get_connector(hostname=hostname, config=config, **kwargs)
    connector.connect()
    connector.authenticate()
    return connector.client


def get_database(name, hostname=None, config=None, **kwargs):
    try:
        client = get_client(hostname=hostname, config=config, **kwargs)
    except Exception as error:
        if "Authentication failed" in str(error):
            raise AuthenticationError(hostname)
        raise
    return client[name]
