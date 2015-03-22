import networkx as nx
import logging

logger = logging.getLogger('methods')

ATTRIBUTE_ADAPTER = 'adapter'

class DBMethod(object):
    
    def __init__(self, callable, expects):
        self._callable = callable
        self._expects = expects

    @property
    def expects(self):
        return self._expects

    def __call__(self, * args, ** kwargs):
        return self._callable(* args, ** kwargs)

    def __repr__(self):
        return "DBMethod(callable={c},expects={e})".format(
            c = self._callable.__name__,
            e = self._expects)

    def name(self):
        return self.__repr__()

class Adapter(object):
    expects = None
    returns = None

    def __call__(self, x):
        assert isinstance(x, self.expects)
        return self.returns(x)

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

class Converter(object):
    
    def __init__(self, adapter_chain):
        self._adapter_chain = adapter_chain

    def convert(self, data):
        for adapter in self._adapter_chain:
            data = adapter()(data)
        return data

    def __len__(self):
        return len(self._adapter_chain)

def get_adapter_chain_from_network(network, source_type, target_type):
    path = nx.shortest_path(network, source_type, target_type)
    for i in range(len(path)-1):
        edge = network[path[i]][path[i+1]]
        yield edge[ATTRIBUTE_ADAPTER]

def get_converter(network, source_type, target_type):
    try:
        adapters = get_adapter_chain_from_network(
            network, source_type, target_type)
    except nx.exception.NetworkXError:
        raise ConversionError(source_type, target_type)
    else:
        return Converter(list(adapters))

def basic_network():
    an = nx.DiGraph()
    import uuid
    an.add_nodes_from([int, float, str, uuid.UUID])
    add_adapter_to_network(an, make_adapter(int, float))
    add_adapter_to_network(an, make_adapter(float, int))
    # to make it interesting...
    #add_adapter_to_network(an, make_adapter(int, str))
    add_adapter_to_network(an, make_adapter(str, int))
    add_adapter_to_network(an, make_adapter(float, str))
    add_adapter_to_network(an, make_adapter(uuid.UUID, str))
    return an

if __name__ == '__main__':
    from matplotlib import pyplot as plt
    plot = nx.draw(basic_network(), with_labels = True)
    plt.show()
