import random
from math import ceil, log


class ID:

    def __init__(self, value: int):
        """
        Kademlia node ID: This is an integer from 0 to 2^160 - 1

        Args:
            value: (int) ID denary value
        """

        self.MAX_ID = 2 ** 160
        self.MIN_ID = 0
        if not (self.MAX_ID > value >= self.MIN_ID):  # ID can be 0, this is used in unit tests.
            raise ValueError(
                f"ID {value} is out of range - must a positive integer less than 2^160."
            )
        self.value = value

    def hex(self) -> str:
        return hex(self.value)

    def denary(self) -> int:
        return self.value

    def bin(self) -> str:
        """
        Returns big-endian value in binary - this does not include a 0b tag at the start.
        :return: Returns the binary value as a string, with length Constants.B by default
        """

        binary = bin(self.value)[2:]
        number_of_zeroes_to_add = ceil(log(self.MAX_ID, 2)) - len(binary)
        padded_binary = number_of_zeroes_to_add * "0" + binary
        return padded_binary

    def set_bit(self, bit: int) -> None:
        """
        Sets a given bit to 1, Little endian. (set_bit(0) sets smallest bit to 0)
        :param bit: bit to be set.
        :return: Nothing
        """
        self.little_endian_bytes()[bit] = "1"

    def big_endian_bytes(self) -> list[str]:
        """
        Returns the padded ID in big-endian binary - largest bit is at index 0.
        """
        return [x for x in self.bin()]

    def little_endian_bytes(self) -> list[str]:
        """
        Returns the padded ID in little-endian binary - smallest bit is at index 0.
        """
        return self.big_endian_bytes()[::-1]

    def __xor__(self, val) -> int:
        if isinstance(val, ID):
            return self.value ^ val.value
        else:
            return self.value ^ val

    def __eq__(self, val) -> bool:
        if isinstance(val, ID):
            return self.value == val.value
        else:
            return self.value == val

    def __ge__(self, val) -> bool:
        if isinstance(val, ID):
            return self.value >= val.value
        else:
            return self.value >= val

    def __le__(self, val) -> bool:
        if isinstance(val, ID):
            return self.value <= val.value
        else:
            return self.value <= val

    def __lt__(self, val) -> bool:
        if isinstance(val, ID):
            return self.value < val.value
        else:
            return self.value < val

    def __gt__(self, val) -> bool:
        if isinstance(val, ID):
            return self.value > val.value
        else:
            return self.value > val

    def __str__(self) -> str:
        return str(self.value)

    def __repr__(self) -> str:
        return str(self.value)[:-3]

    @classmethod
    def max(cls):
        """
        Returns max ID.
        :return: max ID.
        """
        return ID(2**160 - 1)

    @classmethod
    def mid(cls):
        """
        returns middle of the road ID
        :return: middle ID.
        """
        return ID(2**159 - 1)  # Should this be  ID(2**159)? But then ID(1) ^ ID.mid() > ID.mid() ^ ID.max()

    @classmethod
    def min(cls):
        """
        Returns minimum ID.
        :return: minimum ID.
        """
        return ID(0)

    @classmethod
    def random_id_within_bucket_range(cls, bucket):
        """
        Returns an ID within the range of the bucket's low and high range.
        THIS IS NOT AN ID IN THE BUCKETS CONTACT LIST!
        (I mean it could be but shush)

        :param bucket: bucket to be searched
        :return: random ID in bucket.
        """
        return ID(bucket.low() + random.randint(0, bucket.high() - bucket.low()))

    @classmethod
    def random_id(cls, low=0, high=2**160, seed=None):
        """
        Generates a random ID, including both endpoints.

        FOR TESTING PURPOSES.
        Generating random ID's this way will not perfectly spread the prefixes,
        this is a maths law I've forgotten - due to the small scale of this
        I don't particularly see the need to perfectly randomise this.

        If I do though, here's how it would be done:
        - Randomly generate each individual bit, then concatenate.
        """
        if seed:
            random.seed(seed)
        return ID(random.randint(low, high))
