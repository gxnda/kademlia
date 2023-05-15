import hashlib


class Hash:
    def __init__(self):
        self.encrypted = False
        self.hash = None

    def sha_256(self, string: str = None, binary: bytes = None) -> None:
        if string is None and binary is None:
            raise ValueError("Function sha_256() in Hasher must take in either a string under string= or bytes under "
                             "binary=, neither were provided.")
        if string:
            binary = string.encode('utf-8')
        self.hash = hashlib.sha256(binary)
        self.encrypted = True

    def hex_digest(self) -> str:
        if self.hash is None:
            raise ValueError("You have not hashed anything yet! Use a function such as sha_256() to hash.")
        return self.hash.hexdigest()

    def __str__(self):
        return self.hash.hexdigest()


def xor_distance_function(hash1: Hash, hash2: Hash):
    """Takes in 2 hexadecimal hashes and returns the 'exclusive or' between the 2.
    This is used it Kademlia to calculate the 'distance' between 2 nodes."""


a = Hash()
a.sha_256("hello")
print(a)