# Copyright (c) 2016 The Regents of the University of Michigan
# All rights reserved.
# This software is licensed under the BSD 3-Clause License.
import logging
import inspect
import collections

import networkx as nx

from . import conversion
from . import formats
from . import adapters  # noqa required for automagick formats_network

logger = logging.getLogger(__name__)

ATTRIBUTE_ADAPTER = 'adapter'
ATTRIBUTE_WEIGHT = 'weight'


class ConversionNetwork(object):

    def __init__(self, formats_network):
        self.formats_network = formats_network

    def convert(self, src, target_format, debug=False):
        return convert(src, target_format,
                       self.formats_network, debug=debug)

    def converted(self, sources, target_format, ignore_errors=True):
        for doc in converted(sources, target_format,
                             self.formats_network,
                             ignore_errors=ignore_errors):
            yield doc


def get_formats_network():
    """Generate a formats network from all registered formats and adapters.
    Every adapter in the global namespace is automatically registered.
    """
    network = nx.DiGraph()
    network.add_nodes_from(formats.BASICS)
    network.add_nodes_from(formats.BasicFormat.registry.values())
    for adapter in conversion.Adapter.registry.values():
        logger.debug("Adding '{}' to network.".format(adapter()))
        add_adapter_to_network(network, adapter)
    return network


def get_conversion_network():
    return ConversionNetwork(get_formats_network())


def add_adapter_to_network(network, adapter):
    data = network.get_edge_data(
        adapter.expects, adapter.returns,
        default=collections.defaultdict(list))
    data[ATTRIBUTE_ADAPTER].append(adapter)
    weight = data.get(ATTRIBUTE_WEIGHT, adapter.weight)
    data[ATTRIBUTE_WEIGHT] = min(weight, adapter.weight)
    network.add_nodes_from((adapter.expects, adapter.returns))
    network.add_edge(adapter.expects, adapter.returns, data)


class ConversionError(RuntimeError):
    pass


class NoConversionPathError(ConversionError):
    pass


class Converter(object):

    def __init__(self, adapter_chain, source_type, target_type):
        self._source_type = source_type
        self._target_type = target_type
        self._adapter_chain = adapter_chain

    def convert(self, data, debug=False):
        for adapters_ in self._adapter_chain:
            for adapter in sorted(adapters_, key=lambda a: a.weight):
                try:
                    logger.debug(
                        "Attempting conversion with adapter '{}'.".format(
                            adapter()))
                    data = adapter()(data)
                    break
                except Exception as error:
                    logger.debug(
                        "Conversion failed due to error: {}: '{}'.".format(
                            type(error), error))
                    if debug:
                        raise
            else:
                raise ConversionError(self._source_type, self._target_type)
        return data

    def __len__(self):
        return len(self._adapter_chain)

    def __str__(self):
        return "Converter(adapter_chain={},"\
               "source_type={},target_type={})".format(
                   self._adapter_chain, self._source_type, self._target_type)


def _get_adapter_chain_from_path(network, path):
    for i in range(len(path) - 1):
        edge = network[path[i]][path[i + 1]]
        yield edge[ATTRIBUTE_ADAPTER]


def _get_adapter_chains_from_network(network, source_type, target_type):
    paths = nx.shortest_simple_paths(
        network, source_type, target_type, ATTRIBUTE_WEIGHT)
    for path in paths:
        yield _get_adapter_chain_from_path(network, path)


def _get_converters(network, source_type, target_type):
    try:
        for adapter_chain in _get_adapter_chains_from_network(
                network, source_type, target_type):
            yield Converter(list(adapter_chain), source_type, target_type)
    except (nx.exception.NetworkXNoPath, nx.exception.NetworkXError):
        raise NoConversionPathError(source_type, target_type)


def get_converters(network, source_type, target_type):
    mro = inspect.getmro(source_type)
    found_converter = False
    for src_type in mro:
        try:
            for doc in _get_converters(network, src_type, target_type):
                yield doc
        except NoConversionPathError:
            pass
        else:
            found_converter = True
    if not found_converter:
        raise NoConversionPathError(source_type, target_type)


def convert(src, target_format, formats_network, debug=False):
    """Convert the :param src: object to the target_format.

    :param src: Arbitrary source object
    :param target_format: The format to convert to.
    :param formats_network: The network of formats used for conversion.
    """
    if type(src) == target_format:
        return src
    converters = get_converters(formats_network, type(src),
                                target_format)
    for i, converter in enumerate(converters):
        msg = "Attempting conversion path # {}: {} nodes."
        logger.debug(msg.format(i + 1, len(converter)))
        try:
            return converter.convert(src, debug=debug)
        except ConversionError:
            msg = "Conversion attempt with '{}' failed."
            logger.debug(msg.format(converter))
    else:
        raise ConversionError(type(src), target_format)


def converted(sources, target_format, formats_network, ignore_errors=False):
    for src in sources:
        try:
            yield convert(src, target_format, formats_network)
        except NoConversionPathError:
            msg = "No path found."
            logger.debug(msg)
            if not ignore_errors:
                raise
        except ConversionError:
            msg = "Conversion from '{}' to '{}' "\
                  "through available conversion path failed."
            logger.debug(msg.format(type(src), target_format))
            if not ignore_errors:
                raise
        else:
            logger.debug("Success.")
