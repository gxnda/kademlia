import pickle

from kademlia_dht.constants import Constants
from kademlia_dht.errors import DataDecodingError


def encode_data(data: dict) -> bytes:
    """
    Takes in a dictionary, encodes all values using pickle, in order to retain objects
    over HTTP.
    The dictionary is then converted to a string using json.dumps()
    """
    return pickle.dumps(data)


def plain_encode_data(data: dict) -> bytes:
    """
    Takes in a dictionary, encodes all values using pickle, in order to retain objects
    over HTTP.
    The dictionary is then converted to a string using json.dumps()
    """
    return pickle.dumps(data)


def decode_data(encoded_data: bytes) -> dict:
    """
    Takes in a string, decodes all pickled byte strings of the string dictionary 
    into python objects, and returns the decoded dictionary.
    """
    try:
        if isinstance(encoded_data, bytes):
            decoded_data = pickle.loads(encoded_data)
        else:
            raise TypeError(f"Encoded data should be type bytes, found type {type(encoded_data)}")

    except Exception as error:
        raise DataDecodingError("Error decoding data.") from error
    return decoded_data


def plain_decode_data(encoded_data: bytes) -> dict:
    """
    Takes in a string, decodes all pickled byte strings of the string dictionary
    into python objects, and returns the decoded dictionary.
    """
    try:
        if isinstance(encoded_data, bytes):
            decoded_data = pickle.loads(encoded_data)
        else:
            raise TypeError(f"Encoded data should be type bytes, found type {type(encoded_data)}")

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

    my_dict = {"a": 1, "b": 27, "c": [1, 2, 3, MyClass("defined in dict")]}
    print(my_dict)
    enc = encode_data(my_dict)
    dec = decode_data(enc)
    print(dec)
    print(dec["c"][3].method())
    