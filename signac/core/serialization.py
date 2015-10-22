import logging
import sys
import importlib
import hashlib
import inspect
import json

import jsonpickle

logger = logging.getLogger(__name__)

KEY_CALLABLE = 'callable'
KEY_CALLABLE_NAME = 'name'
KEY_CALLABLE_MODULE = 'module'
KEY_CALLABLE_SOURCE_HASH = 'source_hash'
KEY_CALLABLE_MODULE_HASH = 'module_hash'
KEY_CALLABLE_CHECKSUM = 'checksum'


def reload_module(modulename):
    module = importlib.import_module(modulename)
    logger.debug("Reloading module '{}'.".format(module))
    if sys.version_info[0] != 3:
        raise NotImplementedError("Only supported for python versions > 3.x.")
    elif sys.version_info[1] == 3:
        from imp import reload
    elif sys.version_info[1] == 4:
        from importlib import reload
    else:
        assert False
    try:
        reload(module)
    except AttributeError:
        pass


def hash_module(c):
    module = inspect.getmodule(c)
    src_file = inspect.getsourcefile(module)
    m = hashlib.md5()
    with open(src_file, 'rb') as file:
        m.update(file.read())
    return m.hexdigest()


def hash_source(c):
    m = hashlib.md5()
    m.update(inspect.getsource(c).encode())
    return m.hexdigest()


def callable_name(c):
    try:
        return c.__name__
    except AttributeError:
        return c.name()


def callable_spec(c):
    assert callable(c)
    try:
        spec = {
            KEY_CALLABLE_NAME: callable_name(c),
            KEY_CALLABLE_SOURCE_HASH: hash_source(type(c)),
        }
    except TypeError:
        spec = {
            KEY_CALLABLE_NAME: callable_name(c),
            KEY_CALLABLE_MODULE: c.__module__,
            KEY_CALLABLE_MODULE_HASH: hash_module(c),
        }
    return spec


def encode(item):
    return jsonpickle.encode(item).encode()


def decode(binary):
    return jsonpickle.decode(binary.decode())


def encode_callable_filter(fn, args, kwargs):
    b = encode_callable(fn, args, kwargs)
    return {KEY_CALLABLE_CHECKSUM: b[KEY_CALLABLE_CHECKSUM]}


def encode_callable(fn, args, kwargs):
    checksum_src = hash_source(fn)
    doc = dict(
        fn=fn, args=args, kwargs=kwargs,
        module=fn.__module__, checksum_src=checksum_src)
    # we need jsonpickle to encode the functions and
    # json to ensure key sorting
    binary = json.dumps(json.loads(jsonpickle.dumps(doc)),
                        sort_keys=True).encode()
    m = hashlib.sha1()
    m.update(binary)
    checksum = m.hexdigest()
    return {KEY_CALLABLE: binary, KEY_CALLABLE_CHECKSUM: checksum}


def decode_callable(doc, reload=True):
    sys.path.append('')
    binary = doc[KEY_CALLABLE]
    m = hashlib.sha1()
    m.update(binary)
    if m.hexdigest() != doc[KEY_CALLABLE_CHECKSUM]:
        msg = "Checksum deviation!"
        raise RuntimeError(msg)
    try:
        c_doc = jsonpickle.loads(binary.decode())
    except AttributeError as error:
        raise AttributeError(
            "Unable to retrieve callable. Executing from different script? "
            "Error: {}".format(error))
    if reload:
        reload_module(c_doc['module'])
        c_doc = jsonpickle.loads(binary.decode())
    return c_doc['fn'], c_doc['args'], c_doc['kwargs']
