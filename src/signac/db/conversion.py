import logging
import inspect
import collections

import networkx as nx

logger = logging.getLogger(__name__)

ATTRIBUTE_ADAPTER = 'adapter'
ATTRIBUTE_WEIGHT= 'weight'

WEIGHT_DEFAULT = 1
WEIGHT_DISCOURAGED = 10
WEIGHT_STRONGLY_DISCOURAGED = 100

class DBMethod(object):
    expects = None

    def apply(self, arg):
        raise NotImplementedError()

    def __call__(self, arg):
        assert isinstance(arg, self.expects)
        return self.apply(arg)

    def __repr__(self):
        return "{m}.{t}(expects={e})".format(
            m = self.__module__,
            t = type(self),
            e = self.expects)

    def name(self):
        return self.__repr__()

def make_db_method(callable, expected_type):
    class Method(DBMethod):
        expects = expected_type
        def apply(self, arg):
            return callable(arg)
    return Method()

class AdapterMetaType(type):

    def __init__(cls, name, bases, dct):
        if not hasattr(cls, 'registry'):
            cls.registry = dict()
        else:
            identifier = "{}_to_{}".format(cls.expects, cls.returns)
            cls.registry[identifier] = cls

        super().__init__(name, bases, dct)

class Adapter(metaclass = AdapterMetaType):
    expects = None
    returns = None
    weight = WEIGHT_DEFAULT

    def __call__(self, x):
        assert isinstance(x, self.expects)
        return self.convert(x)

    def convert(self, x):
        return self.returns(x)

    def __str__(self):
        return "{n}(from={f},to={t})".format(
            n = self.__class__,
            f = self.expects,
            t = self.returns)

def make_adapter(src, dst, convert = None, w = None):
    class BasicAdapter(Adapter):
        expects = src
        returns = dst
        if w is not None:
            weight = w
        if convert is not None:
            def __call__(self, x):
                return convert(x)
    return BasicAdapter

def add_adapter_to_network(network, adapter):
    data = network.get_edge_data(
        adapter.expects, adapter.returns,
        default = collections.defaultdict(list))
    data[ATTRIBUTE_ADAPTER].append(adapter)
    weight = data.get(ATTRIBUTE_WEIGHT, adapter.weight)
    data[ATTRIBUTE_WEIGHT] = min(weight, adapter.weight)
    network.add_edge(
        adapter.expects,
        adapter.returns,
        data)

class ConversionError(Exception):
    pass

class NoConversionPath(ConversionError):
    pass

class Converter(object):
    
    def __init__(self, adapter_chain, source_type, target_type):
        self._source_type = source_type
        self._target_type = target_type
        self._adapter_chain = adapter_chain

    def convert(self, data):
        for adapters in self._adapter_chain:
            for adapter in sorted(adapters, key=lambda a: a.weight):
                try:
                    data = adapter()(data)
                    break
                except Exception as error:
                    logger.debug("Conversion failed due to error: '{}'.".format(error))
            else:
                raise ConversionError(self._source_type, self._target_type)
        return data

    def __len__(self):
        return len(self._adapter_chain)

    def __str__(self):
        return "Converter(adapter_chain={},source_type={},target_type={})".format(
            self._adapter_chain, self._source_type, self._target_type)

def _get_adapter_chain_from_path(network, path):
    for i in range(len(path)-1):
        edge = network[path[i]][path[i+1]]
        yield edge[ATTRIBUTE_ADAPTER]

def _get_adapter_chains_from_network(network, source_type, target_type):
    paths = nx.shortest_simple_paths(network, source_type, target_type, ATTRIBUTE_WEIGHT)
    for path in paths:
        yield _get_adapter_chain_from_path(network, path)

def _get_converters(network, source_type, target_type):
    try:
        for adapter_chain in _get_adapter_chains_from_network(
            network, source_type, target_type):
            yield Converter(list(adapter_chain), source_type, target_type)
    except (nx.exception.NetworkXNoPath, nx.exception.NetworkXError) as error:
        raise NoConversionPath(source_type, target_type) from error

def get_converters(network, source_type, target_type):
    mro = inspect.getmro(source_type)
    for src_type in mro:
        yield from _get_converters(network, src_type, target_type)
    else:
        raise NoConversionPath(source_type, target_type)

class FormatMetaType(type):

    def __init__(cls, name, bases, dct):
        if not hasattr(cls, 'registry'):
            cls.registry = dict()
        else:
            cls.registry[name] = cls

        super().__init__(name, bases, dct)

class BasicFormat(metaclass = FormatMetaType):
    pass
