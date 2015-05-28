import logging
logger = logging.getLogger(__name__)

KEY_CALLABLE = 'callable'
KEY_CALLABLE_NAME = 'name'
KEY_CALLABLE_MODULE = 'module'
KEY_CALLABLE_SOURCE_HASH = 'source_hash'
KEY_CALLABLE_MODULE_HASH = 'module_hash'
KEY_CALLABLE_CHECKSUM = 'checksum'

import hashlib
import pickle as serializer

def reload_module(modulename):
    import sys, importlib
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
    reload(module)

def hash_module(c):
    import inspect
    module = inspect.getmodule(c)
    src_file = inspect.getsourcefile(module)
    m = hashlib.md5()
    with open(src_file, 'rb') as file:
        m.update(file.read())
    return m.hexdigest()

def hash_source(c):
    import inspect
    m = hashlib.md5()
    m.update(inspect.getsource(c).encode())
    return m.hexdigest()

def callable_name(c):
    try:
        return c.__name__
    except AttributeError:
        return c.name()

def callable_spec(c):
    import inspect
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
    import jsonpickle
    return jsonpickle.encode(item).encode()

def decode(binary):
    import jsonpickle
    return jsonpickle.decode(binary.decode())

def encode_callable(fn, args, kwargs):
    checksum_src = hash_source(fn)
    binary = serializer.dumps(
        {'fn': fn, 'args': args, 'kwargs': kwargs,
         'module': fn.__module__,
         'src': checksum_src},
         protocol = serializer.HIGHEST_PROTOCOL)
    checksum = hashlib.sha1()
    checksum.update(binary)
    return {
        KEY_CALLABLE: binary,
        KEY_CALLABLE_CHECKSUM: checksum.hexdigest()}

def decode_callable(doc, reload = True):
    import sys
    sys.path.append('')
    import warnings
    binary = doc['callable']
    checksum = doc[KEY_CALLABLE_CHECKSUM]
    m = hashlib.sha1()
    m.update(binary)
    if not checksum == m.hexdigest():
        msg = "Checksum deviation! Possible security breach!"
        raise RuntimeError(msg)
    c_doc = serializer.loads(binary)
    if reload:
        reload_module(c_doc['module'])
        c_doc = serializer.loads(binary)
    else:
        fn = c_doc['fn']
        if not hash_source(c_doc['fn']) == c_doc['src']:
            msg = "Source checksum deviates. Source code was changed."
            warnings.warn(msg, RuntimeWarning)
    return c_doc['fn'], c_doc['args'], c_doc['kwargs']
