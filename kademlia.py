from datetime import datetime
from abc import abstractmethod
import random
from threading import Lock

# Errors


class TooManyContactsError(Exception):
    """Raised when a contact is added to a full k-bucket."""
    pass


class OutOfRangeError(Exception):
    """Raised when a contact is added to a k-bucket that is out of range."""
    pass


class OurNodeCannotBeAContactError(Exception):
    """Raised when a contact added has the same ID as the client."""


class AllKBucketsAreEmptyError(Exception):
    """Raised when no KBuckets can be iterated through."""


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
        """
        Returns value in binary - this includes a 0b tag at the start.
        Do [2:] to remove this.
        """
        return bin(self.value)

    def big_endian_bytes(self) -> list:
        """
        Returns the ID in big-endian binary - largest bit is at index 0.
        """
        big_endian = [x for x in id.bin()[2:]]
        return big_endian

    def little_endian_bytes(self) -> list:
        """
        Returns the ID in little-endian binary - smallest bit is at index 0.
        """
        big_endian = [x for x in id.bin()[2:]][::-1]
        return big_endian

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

    def store(self, key: ID, sender: Contact, value: str) -> None:
        # !!! TO BE IMPLEMENTED
        pass

    def find_node(self, key: ID, sender: Contact) -> tuple[list[Contact], str]:
        # !!! TO BE IMPLEMENTED
        pass

    def find_value(self, key: ID, sender: Contact):  # -> (list[Contact], str)
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

    def __init__(self, k=Constants().K, low=0, high=2**160):
        """
        Initialises a k-bucket with a specific ID range, 
        initially from 0 to 2**160.
        """
        self.contacts: list[Contact] = []
        self._low = low
        self._high = high
        self.time_stamp: datetime
        self._k = k
        self.lock = Lock()

    def is_full(self):
        return len(self.contacts) >= self._k

    def contains(self, id: ID) -> bool:
        """
        Returns boolean determining whether a given contact ID is in the kbucket.
        """

        # replacable
        return any(id == contact.id for contact in self.contacts)

    def touch(self):
        self.time_stamp = datetime.now()

    def has_in_range(self, id: ID):
        return self._low <= id.value <= self._high

    def add_contact(self, contact: Contact):
        # TODO: Check if this is meant to check if it exists in the bucket.
        if self.is_full():
            raise TooManyContactsError("KBucket is full.")
        elif not self.has_in_range(contact.id):
            raise OutOfRangeError("Contact ID is out of range.")
        else:
            # !!! should this check whether or not the contact is already in the bucket?
            self.contacts.append(contact)

    def can_split(self) -> bool:
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
        with self.lock:
            # TODO: What is self.node?
            return (self.has_in_range(self.node.id)
                    or (self.depth() % Constants().B != 0))

    def depth(self) -> int:
        """
        "The depth is just the length of the prefix shared by all nodes in 
        the k-bucket’s range.” Do not confuse that with this statement in the 
        spec: “Define the depth, h, of a node to be 160 - i, where i is the 
        smallest index of a nonempty bucket.” The former is referring to the 
        depth of a k-bucket, the latter the depth of the node.
        """

        return len(self.shared_bits())

    def shared_bits(self) -> str:
        """
        Return the longest shared binary prefix between all 
        contacts in the kbucket. This does not "0b" before the binary.
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

        return longest_prefix

    def split(self) -> tuple:
        """
        Splits KBucket in half, returns tuple of type (KBucket, KBucket).
        """
        midpoint = (self._low + self._high) // 2
        k1: KBucket = KBucket(low=self._low, high=midpoint)
        k2: KBucket = KBucket(low=midpoint, high=self._high)

        for c in self.contacts:
            if c.id < midpoint:
                k1.add_contact(c)
            else:
                k2.add_contact(c)

        return (k1, k2)


class BucketList:

    def __init__(self, our_id: ID):
        self.buckets: list[KBucket] = [KBucket()]
        # first k-bucket has max range
        self.our_id: ID = our_id

        # create locking object
        self.lock = Lock()

    def _get_kbucket_index(self, other_id: ID) -> int:
        """
        Returns the first kbuckets index in the bucket list 
        which has a given ID in range. Returns -1 if not found.
        """

        with self.lock:
            for i in range(len(self.buckets)):
                if self.buckets[i].has_in_range(other_id):
                    return i
            return -1

    def add_contact(self, contact: Contact) -> None:
        if self.our_id == contact.id:
            raise OurNodeCannotBeAContactError(
                "Cannot add ourselves as a contact.")

        contact.touch()  # Update the time last seen to now

        with self.lock:
            kbucket: KBucket = get_kbucket(contact.id)
            if kbucket.contains(contact.id):
                # replace contact, then touch it
                kbucket.replace_contact(contact)

            elif kbucket.is_full():

                if kbucket.can_split():
                    # Split then try again
                    k1, k2 = kbucket.split()
                    index: int = self._get_kbucket_index(contact.id)
                    self.buckets[index] = k1
                    self.buckets.insert(index + 1, k2)
                    self.add_contact(contact)

                else:
                    # TODO: Ping the oldest contact to see if it's
                    # still around and replace it if not.
                    pass

            else:
                # Bucket is not full, nothing special happens.
                kbucket.add_contact(contact)


def random_id_in_space(low=0, high=2**160):
    """
    FOR TESTING PURPOSES.
    TODO: Remove.
    Generating random ID's this way will not perfectly spread the prefixes,
    this is a maths law I've forgotten - due to the small scale of this 
    I don't particularly see the need to perfectly randomise this.

    If I do though, here's how it would be done:
    - Randomly generate each individual bit, then concatenate.
    """
    return ID(random.randint(low, high))


def find_closest_nonempty_kbucket(key: ID) -> KBucket:
    """
    Helper method.
    Code listing 34.
    """
    # gets all non empty buckets from bucket list
    non_empty_buckets: list[KBucket] = [
        b for b in node.bucket_list.buckets if (len(b.contacts) != 0)
    ]
    if len(non_empty_buckets) == 0:
        raise AllKBucketsAreEmptyError("No non-empty buckets can be found.")

    def sort_key(bucket):
        return bucket.id ^ key

    return sorted(non_empty_buckets, key=(lambda b: b.id.value ^ key.value))[0]


def get_closest_nodes(key: ID, bucket: KBucket) -> list[Contact]:
    return sorted(bucket.contacts, key=lambda c: c.id.value ^ key.value)


def get_closer_nodes(key: ID, 
                     node_to_query: Contact, 
                     rpc_call: object, 
                     closer_contacts: list[Contact],
                     farther_contacts: list[Contact],
                     val: str,
                     found_by: Contact) -> bool:

    contacts, c_found_by, found_val = rpc_call(key, node_to_query)
    val = found_val
    found_by = c_found_by
    peers_nodes: list[Contact] = contacts
    


if __name__ == "__main__":
    id = ID(1234)
    print(id.big_endian_bytes())
    print(id.little_endian_bytes())
