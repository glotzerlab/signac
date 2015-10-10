import re
import subprocess
import queue
import itertools
from threading import Thread, Event
from math import tanh


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


def fetch(target, timeout=None, stop_event=None):
    tmp_queue = queue.Queue()
    if stop_event is None:
        stop_event = Event()

    def inner_loop():
        w = (tanh(0.05 * i) for i in itertools.count())
        while(not stop_event.is_set()):
            result = target()
            if result is not None:
                tmp_queue.put(result)
                return
        stop_event.wait(max(0.001, next(w)))
    thread_fetch = Thread(target=inner_loop)
    thread_fetch.start()
    try:
        thread_fetch.join(timeout=timeout)
    except KeyboardInterrupt:
        stop_event.set()
        thread_fetch.join()
        raise
    if thread_fetch.is_alive():
        stop_event.set()
        thread_fetch.join()
    try:
        return tmp_queue.get_nowait()
    except queue.Empty:
        raise TimeoutError()


def mongodb_fetch_find_one(collection, spec, timeout=None):
    def target():
        return collection.find_one(spec)
    return fetch(target=target, timeout=timeout)


class Version(dict):
    """Utility class to manage revision control numbers."""

    def __init__(self, major=0, minor=0, change=0, postrelease='', prerelease='final'):
        if prerelease > 'final':
            raise ValueError('illegal pre-release tag', prerelease)
        super(Version, self).__init__(major=major, minor=minor,
                                      change=change, postrelease=postrelease, prerelease=prerelease)

    def to_tuple(self):
        return self['major'], self['minor'], self['change'], self['prerelease'], self['postrelease']

    def __lt__(self, other):
        return self.to_tuple() < other.to_tuple()

    def __eq__(self, other):
        return self.to_tuple() == other.to_tuple()

    def __str__(self):
        return '{major}.{minor}{postrelease}.{change}{prerelease}'.format(**self)

    def __repr__(self):
        return "Version({})".format(','.join(('{}={}'.format(k, v) for k, v in self.items())))


def parse_version(version_str):
    """Parse a version number into a version object."""
    p = re.compile(
        r"(?P<major>[0-9]*)\.(?P<minor>[0-9]*)((?P<postrelease>-?\w*)\.(?P<change>[0-9])(?P<prerelease>\w*))?")
    r = p.match(version_str)
    v = r.groupdict()
    version = Version(**{
        'major': int(v.get('major') or 0),
        'minor': int(v.get('minor') or 0),
        'change': int(v.get('change') or 0),
        'postrelease': str(v.get('postrelease') or ''),
        'prerelease': str(v.get('prerelease') or 'final'),
    })
    return version
