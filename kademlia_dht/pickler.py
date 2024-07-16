import json
import logging
import pickle

from kademlia_dht.constants import Constants
from kademlia_dht.errors import DataDecodingError


logger = logging.getLogger("__main__")


class Encoder(json.JSONEncoder):
    def default(self, obj):
        if hasattr(obj, "encode_into_json"):
            logger.debug(f"Encoding object {type(obj)} with method 'encode'.")
            return obj.encode()

        logger.debug(f"Encoding object {type(obj)} with method 'default'.")
        return json.JSONEncoder.default(self, obj)


def encode_data(data: dict) -> str:
    """
    Takes in a dictionary, encodes all values using pickle, in order to retain objects
    over HTTP.
    The dictionary is then converted to a string using json.dumps()
    """

    return json.dumps(data, cls=Encoder)


def decode_data(encoded_data: str | bytes) -> dict:
    """
    Takes in a string, decodes all pickled byte strings of the string dictionary 
    into python objects, and returns the decoded dictionary.
    """

    def object_hook(obj):
        if isinstance(obj, dict) and "can_decode_into_json" in obj:
            logger.debug(f"Decoding object {type(obj)} with method 'decode'.")

            return obj.decode()

        logger.debug(f"Decoding object {type(obj)} with method 'default'.")
        return obj

    try:
        if isinstance(encoded_data, str):
            decoded_data = json.loads(encoded_data, object_hook=object_hook)
        elif isinstance(encoded_data, bytes):
            decoded_data = json.loads(encoded_data.decode(Constants.PICKLE_ENCODING), object_hook=object_hook)
        else:
            raise TypeError(f"Encoded data should be type str, found type {type(encoded_data)}")

    except Exception as error:
        raise DataDecodingError("Error decoding data.") from error
    return decoded_data


def encode_dict_as_str(data: dict) -> str:
    return pickle.dumps(data).decode(Constants.PICKLE_ENCODING)


if __name__ == "__main__":
    class MyClass:
        def __init__(self, defined):
            self.static_attr = "static"
            self.defined_attr = defined
            self._protected_attr = "protected"
            self.__private_attr = "private"

        def method(self):
            return self.__private_attr, self.defined_attr

        def encode_into_json(self):
            return {
                "can_decode_into_json": True,
                "type": str(type(self)),
                "defined_attr": self.defined_attr,
                "protected_attr": self._protected_attr,
                "private_attr": self.__private_attr
            }

        @classmethod
        def decode(cls, encoded_data):
            decoded_data = cls(defined=encoded_data["defined_attr"])
            decoded_data.static_attr = encoded_data["static_attr"]
            decoded_data._protected_attr = encoded_data["protected_attr"]
            decoded_data.__private_attr = encoded_data["private_attr"]
            return decoded_data

    my_dict = {"a": 1, "b": 27, "c": [1, 2, 3, MyClass("defined in dict")]}
    print(my_dict)
    enc = encode_data(my_dict)
    print(enc)
    dec = decode_data(enc)
    print(dec)
    print(dec["c"][3].method())
