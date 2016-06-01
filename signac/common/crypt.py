import base64

from .passlib.context import CryptContext


class SimplePasswordCache(object):

    def __init__(self):
        self._cache = dict()

    @classmethod
    def _encode(cls, msg):
        return base64.b64encode(msg)

    @classmethod
    def _decode(cls, msg):
        return base64.b64decode(msg)

    def __contains__(self, key):
        return key in self._cache

    def __set__(self, key, value):
        self._cache[key] = self._encode(self._secret, value)

    def __getitem__(self, key):
        return self._decode(self._cache.__getitem__(key))

    def setdefault(self, key, value):
        return self._decode(self._cache.setdefault(key, self._encode(value)))


def get_crypt_context():
    return CryptContext(schemes=('bcrypt', ))


def parse_pwhash(pwhash):
    if get_crypt_context().identify(pwhash) == 'bcrypt':
        return dict(
            rounds=int(pwhash.split('$')[2]),
            salt=pwhash[-53:-31])
    else:
        raise ValueError(pwhash)
