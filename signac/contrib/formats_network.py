import logging

import networkx as nx

from . import conversion
from . import formats

logger = logging.getLogger(__name__)


class ConversionNetwork(object):

    def __init__(self, formats_network):
        self.formats_network = formats_network

    def convert(self, src, target_format, debug=False):
        return conversion.convert(src, target_format,
                                  self.formats_network, debug=debug)

    def converted(self, sources, target_format, ignore_errors=True):
        yield from conversion.converted(sources, target_format,
                                        self.formats_network,
                                        ignore_errors=ignore_errors)


def get_formats_network():
    """Generate a formats network from all registered formats and adapters.
    Every adapter in the global namespace is automatically registered.
    """
    network = nx.DiGraph()
    network.add_nodes_from(formats.BASICS)
    network.add_nodes_from(formats.BasicFormat.registry.values())
    for adapter in conversion.Adapter.registry.values():
        logger.debug("Adding '{}' to network.".format(adapter()))
        conversion.add_adapter_to_network(
            network, adapter)
    return network


def get_conversion_network():
    return ConversionNetwork(get_formats_network())
