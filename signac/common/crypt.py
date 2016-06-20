# Copyright (c) 2016 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
import base64

from . import six

try:
    from passlib.context import CryptContext
except ImportError:
    def get_crypt_context():
        "This function requires passlib!"
        return None
else:
    def get_crypt_context():
        "Return the default signac crypto context."
        return CryptContext(schemes=('bcrypt', ))

try:
    import keyring
except ImportError:
    def get_keyring():
        "This function requires keyring!"
        return None
else:
    def get_keyring():
        "Return the system user keyring."
        return keyring.get_keyring()


class SimpleKeyring(object):
    """Simple in-memory keyring for caching."""

    def __init__(self):
        self._cache = dict()

    @classmethod
    def _encode(cls, msg):
        if msg is None:
            return
        if six.PY2:
            return base64.b64encode(msg)
        else:
            return base64.b64encode(msg.encode())

    @classmethod
    def _decode(cls, msg):
        if msg is None:
            return
        if six.PY2:
            return base64.b64decode(msg)
        else:
            return base64.b64decode(msg).decode()

    def __contains__(self, key):
        return key in self._cache

    def __set__(self, key, value):
        self._cache[key] = self._encode(self._secret, value)

    def __getitem__(self, key):
        return self._decode(self._cache.__getitem__(key))

    def setdefault(self, key, value):
        return self._decode(self._cache.setdefault(key, self._encode(value)))


def parse_pwhash(pwhash):
    "Extract hash configuration from hash string."
    if get_crypt_context().identify(pwhash) == 'bcrypt':
        return dict(
            rounds=int(pwhash.split('$')[2]),
            salt=pwhash[-53:-31])
