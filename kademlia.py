from datetime import datetime
from abc import abstractmethod


class TooManyContactsError(Exception):
    """Raised when a contact is added to a full k-bucket."""
    pass


class OutOfRangeError(Exception):
    """Raised when a contact is added to a k-bucket that is out of range."""
    pass


class Constants:

    def __init__(self):
        self.K = 20  # Maximum K-Bucket size
        self.B = 5  # something to do with splitting buckets


class ID:

    def __init__(self, value: int):
        """
        Kademlia node ID: This is an integer from 0 to 2^160 - 1

        Args:
            value: (int) ID denary value
        """

        two_160 = 1461501637330902918203684832716283019655932542976
        self.MAX_ID = two_160  # 2^160
        self.MIN_ID = 0
        if not (value < self.MAX_ID and value >= self.MIN_ID):
            # TODO: check if value >= self.MIN_ID is valid.
            raise ValueError(
                f"ID {value} is out of range - must a positive integer less than 2^160."
            )
        self.value = value

    def hex(self) -> str:
        return hex(self.value)

    def denary(self) -> int:
        return self.value

    def bin(self) -> str:
        return bin(self.value)

    def __str__(self) -> str:
        return str(self.value)


class IStorage:
    """Interface which 'abstracts the storage mechanism for key-value pairs.''"""

    @abstractmethod
    def contains(self, key: ID) -> bool:
        pass

    @abstractmethod
    def try_get_value(self, key: ID, out: (int or str)) -> bool:
        pass

    @abstractmethod
    def get(self, key: (ID or int)) -> str:
        pass

    @abstractmethod
    def get_timestamp(self, key: int) -> datetime:
        pass

    @abstractmethod
    def set(self, key: ID, value: str, expiration_time_sec: int = 0) -> None:
        pass

    @abstractmethod
    def get_expiration_time_sec(self, key: int) -> int:
        pass

    @abstractmethod
    def remove(self, key: int) -> None:
        pass

    @abstractmethod
    def get_keys(self) -> list[int]:
        pass

    @abstractmethod
    def touch(self, key: int) -> None:
        pass


class Contact:

    def __init__(self, contact_ID: ID, protocol=None):
        self.protocol = protocol
        self.id = contact_ID
        self.last_seen: datetime = datetime.now()

    def touch(self):
        """Updates the last time the contact was seen."""
        self.last_seen = datetime.now()


class Node:

    def __init__(self,
                 contact: Contact,
                 storage: IStorage,
                 cache_storage=None):
        self._ourContact: Contact = contact
        self._bucket_list = BucketList(contact.id)
        self._storage: IStorage = storage

    def ping(self, sender: Contact) -> Contact:
        # !!! TO BE IMPLEMENTED
        pass

    def store(self, sender: Contact, key: ID, value: str) -> None:
        # !!! TO BE IMPLEMENTED
        pass

    def find_node(self, sender: Contact, key: ID) -> tuple[list[Contact], str]:
        # !!! TO BE IMPLEMENTED
        pass

    def find_value(self, sender: Contact, key: ID):  # -> (list[Contact], str)
        # !!! TO BE IMPLEMENTED
        pass


class DHT:

    def __init__(self):
        self._base_router = None

    def router(self):
        return self._base_router


class Router:

    def __init__(self, node: Node) -> None:
        self.node = node


class KBucket:

    def __init__(self, k=20):
        """Initialises a k-bucket with a specific ID range, initially from 0 to 2**160."""
        self.contacts: list[Contact] = []
        self._low = 0
        self._high = 2**160
        self.time_stamp: datetime
        self._k = k

    def bucket_full(self):
        return len(self.contacts) >= self._k

    def touch(self):
        self.time_stamp = datetime.now()

    def is_in_range(self, contact: Contact):
        return self._low <= contact.id.value <= self._high

    def add_contact(self, contact: Contact):
        # TODO: Check if this is meant to check if it exists in the bucket.
        if self.bucket_full():
            raise TooManyContactsError("KBucket is full.")
        elif not self.is_in_range(contact):
            raise OutOfRangeError("Contact ID is out of range.")
        else:
            # !!! should this check whether or not the contact is already in the bucket?
            self.contacts.append(contact)

    def can_split(self) -> bool:
        # !!! TO BE IMPLEMENTED
        # kbucket.HasInRange(ourID) || ((kbucket.Depth() % Constants.B) != 0)
        """
        The depth to which the bucket has split is based on the number of bits 
        shared in the prefix of the contacts in the bucket. With random IDs,    
        this number will initially be small, but as bucket ranges become more 
        narrow from subsequent splits, more contacts will begin the share the 
        same prefix and the bucket when split, will result in less “room” for 
        new contacts. Eventually, when the bucket range becomes narrow enough, 
        the number of bits shared in the prefix of the contacts in the bucket 
        reaches the threshold b, which the spec says should be 5.
        """
        # not taken from book, made myself
        return (self.is_in_range(self.node.id)
                or self.depth() % Constants().B != 0)

    def depth(self):
        """
        "The depth is just the length of the prefix shared by all nodes in 
        the k-bucket’s range.” Do not confuse that with this statement in the 
        spec: “Define the depth, h, of a node to be 160 - i, where i is the 
        smallest index of a nonempty bucket.” The former is referring to the 
        depth of a k-bucket, the latter the depth of the node.
        """

        def longest_shared_prefix_str(a: str, b: str) -> str:
            """Returns the longest common prefix between two strings."""

            if len(a) < len(b):  # swap a and b if a is shorter
                a, b = b, a

            for i in range(len(b)):
                if a[i] != b[i]:
                    return a[:i]

            return b

        longest_prefix = self.contacts[0].id.bin()[2:]  # first node id
        # print(self.contacts[0].id.bin()[2:], longest_prefix)
        for contact in self.contacts[1:]:
            id_bin = contact.id.bin()[2:]
            # print(id_bin)
            longest_prefix = longest_shared_prefix_str(id_bin, longest_prefix)
            # print(longest_prefix)

        return len(longest_prefix)


class BucketList:

    def __init__(self, our_id: ID):
        self.buckets: list[KBucket] = [KBucket()]
        # first k-bucket has max range
        self.our_id: ID = our_id

    def add_contact(self, contact: Contact) -> None:
        # !!! TO BE IMPLEMENTED
        pass


if __name__ == "__main__":
    bucket = KBucket(k=64)
    #bucket.add_contact(Contact(ID(5)))
    bucket.add_contact(Contact(ID(6)))

    bucket.add_contact(Contact(ID(12)))
    bucket.add_contact(Contact(ID(13)))
    bucket.add_contact(Contact(ID(14)))
    bucket.add_contact(Contact(ID(15)))

    print(bucket.contacts)
    depth = bucket.depth()
    print(f"depth: {depth}")
