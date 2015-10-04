import logging
import hashlib
import json

import jsonpickle
from pymongo.errors import DuplicateKeyError

from .mongodb_queue import MongoDBQueue

logger = logging.getLogger(__name__)

KEY_CHECKSUM = 'checksum'
KEY_ELEMENT = 'element'

def encode_element(element):
    binary = json.dumps(json.loads(jsonpickle.dumps(element)), sort_keys=True).encode()
    return binary

def decode_element(binary):    
    return jsonpickle.loads(binary.decode())

def calculate_checksum(binary):
    m = hashlib.sha1()
    m.update(binary)
    return m.hexdigest()

class MongoDBSet(object):
    
    def __init__(self, collection):
        """Initialize an instance of MongoDBSet.

        :param collection: The MongoDB collection to be used for this set.
        """
        self._collection = collection

    def __len__(self):
        """Return the size of the set."""
        return self._collection.find().count()

    def _filter(self, elem):
        return {'_id': calculate_checksum(encode_element(elem))}

    def __contains__(self, elem):
        """Returns true if elem is a member of the set."""
        return self._collection.find_one(self._filter(elem)) is not None
    
    def add(self, elem):
        """Add element to the set, if not already present."""
        binary = encode_element(elem)
        try:
            decoded = decode_element(binary)
            if elem != decoded:
                raise ValueError((elem, decoded))
        except Exception as error:
            msg = "Attempt to encode element '{}' failed: {}."
            raise ValueError(msg.format(elem, error))
        checksum = calculate_checksum(binary)
        doc = {
            '_id': checksum,
            KEY_ELEMENT : binary}
        try:    
            result = self._collection.insert_one(doc)
        except DuplicateKeyError:
            pass

    def remove(self, elem):
        """Remove element from the set.
        
        :raises KeyError if elem is not member of the set."""
        result = self._collection.delete_one(self._filter(elem))
        if not result.deleted_count:
            raise KeyError(elem)

    def discard(self, elem):
        """Remove element from the set if it is present."""
        self._collection.delete_one(self._filter(elem))

    def pop(self):
        """Remove and return an arbitrary element from the set.

        :raises KeyError if the set is empty.
        """
        document = self._collection.find_one_and_delete({})
        if document is None:
            raise KeyError()
        else:
            return decode_element(document[KEY_ELEMENT])

    def clear(self):
        """Remove all elements from the set."""
        self._collection.delete_many({})
