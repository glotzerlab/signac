from .passlib.context import CryptContext


def get_crypt_context():
    return CryptContext(schemes=('bcrypt', ))


def parse_pwhash(pwhash):
    if get_crypt_context().identify(pwhash) == 'bcrypt':
        return dict(
            rounds=int(pwhash.split('$')[2]),
            salt=pwhash[-53:-31])
    else:
        raise ValueError(pwhash)
