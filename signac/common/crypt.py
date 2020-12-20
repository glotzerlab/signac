# Copyright (c) 2017 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
import base64

from deprecation import deprecated

from ..version import __version__

try:
    from passlib.context import CryptContext
except ImportError:

    def get_crypt_context():
        "This function requires passlib!"
        return None


else:

    def get_crypt_context():
        "Return the default signac crypto context."
        return CryptContext(schemes=("bcrypt",))


try:
    import keyring
except ImportError:

    def get_keyring():
        "This function requires keyring!"
        return None


else:

    def get_keyring():
        "Return the system user keyring."
        # In some newer versions of keyring (probably >=21.2.0), no backend is
        # available, which causes problems for signac's implementation. This
        # signac feature is already deprecated so this is only enough of a fix
        # to prevent tests from failing for users with new versions of keyring
        # installed.
        try:
            kr = keyring.get_keyring()
        except RuntimeError:
            return None
        if kr.priority <= 0 or isinstance(kr, keyring.backends.fail.Keyring):
            return None
        else:
            return kr


"""
THIS MODULE IS DEPRECATED!
"""


@deprecated(
    deprecated_in="1.3",
    removed_in="2.0",
    current_version=__version__,
    details="The crypt module is deprecated.",
)
class SimpleKeyring:
    """Simple in-memory keyring for caching."""

    def __init__(self):
        self._cache = {}

    @classmethod
    def _encode(cls, msg):
        if msg is None:
            return
        return base64.b64encode(msg.encode())

    @classmethod
    def _decode(cls, msg):
        if msg is None:
            return
        return base64.b64decode(msg).decode()

    def __contains__(self, key):
        return key in self._cache

    def __set__(self, key, value):
        self._cache[key] = self._encode(self._secret, value)

    def __getitem__(self, key):
        return self._decode(self._cache.__getitem__(key))

    def setdefault(self, key, value):
        return self._decode(self._cache.setdefault(key, self._encode(value)))


@deprecated(
    deprecated_in="1.3",
    removed_in="2.0",
    current_version=__version__,
    details="The crypt module is deprecated.",
)
def parse_pwhash(pwhash):
    "Extract hash configuration from hash string."
    if get_crypt_context().identify(pwhash) == "bcrypt":
        return dict(rounds=int(pwhash.split("$")[2]), salt=pwhash[-53:-31])
