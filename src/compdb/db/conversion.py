import networkx as nx
import logging
logger = logging.getLogger(__name__)

ATTRIBUTE_ADAPTER = 'adapter'

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

def make_adapter(src, dst, convert = None):
    class BasicAdapter(Adapter):
        expects = src
        returns = dst
        if convert is not None:
            def __call__(self, x):
                return convert(x)
    return BasicAdapter

def add_adapter_to_network(network, adapter):
    network.add_edge(
        adapter.expects,
        adapter.returns,
        {ATTRIBUTE_ADAPTER: adapter})

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
        try:
            for adapter in self._adapter_chain:
                data = adapter()(data)
            return data
        except Exception as error:
            raise ConversionError(self._source_type, self._target_type) from error

    def __len__(self):
        return len(self._adapter_chain)

def get_adapter_chain_from_network(network, source_type, target_type):
    path = nx.shortest_path(network, source_type, target_type)
    for i in range(len(path)-1):
        edge = network[path[i]][path[i+1]]
        yield edge[ATTRIBUTE_ADAPTER]

def _get_converter(network, source_type, target_type):
    try:
        adapters = get_adapter_chain_from_network(
            network, source_type, target_type)
        return Converter(list(adapters), source_type, target_type)
    except (nx.exception.NetworkXNoPath, nx.exception.NetworkXError) as error:
        raise NoConversionPath(source_type, target_type) from error

def get_converter(network, source_type, target_type):
    import inspect
    mro = inspect.getmro(source_type)
    for src_type in mro:
        try:
            converter = _get_converter(network, src_type, target_type)
        except ConversionError as error:
            pass
        else:
            return converter
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
