import pickle
from cryptography.fernet import Fernet

from kademlia.errors import DataDecryptingError


def encrypt_data(data: dict, key: bytes) -> bytes:
    """
    Takes in a dictionary, encodes all values using pickle, in order to retain objects
    over HTTP, Then it is encrypted using Fernet.
    """
    f = Fernet(key)
    return f.encrypt(pickle.dumps(data))


def plain_encode_data(data: dict) -> bytes:
    """
    Takes in a dictionary, encodes all values using pickle, in order to retain objects
    over HTTP.
    The dictionary is then converted to a string using json.dumps()
    """
    return pickle.dumps(data)


def decrypt_data(encrypted_data: bytes, key: bytes) -> dict:
    """
    Takes in a string, decodes all pickled byte strings of the string dictionary 
    into python objects, and returns the decoded dictionary.
    """
    f = Fernet(key)
    try:
        if isinstance(encrypted_data, bytes):
            decoded_data = pickle.loads(f.decrypt(encrypted_data))
        else:
            raise TypeError(f"Encoded data should be type bytes, found type {type(encrypted_data)}")

    except Exception as error:
        raise DataDecryptingError("Error decrypting data.") from error

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
        raise DataDecryptingError("Error decoding data.") from error
    return decoded_data


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
    key = Fernet.generate_key()
    enc = encrypt_data(my_dict, key)
    dec = decrypt_data(enc, key)
    print(dec)
    print(dec["c"][3].method())
    