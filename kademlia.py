import abc
import random
from abc import abstractmethod
from datetime import datetime, timedelta
from statistics import median_high
from typing import Callable, TypedDict
from dataclasses import dataclass
# from typing_extensions import override
import pickle

# from threading import Lock

DEBUG: bool = True

if DEBUG:
    random.seed(1)  # For consistent testing

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


class SendingQueryToSelfError(Exception):
    """Raised when a Query (RPC Call) is sent to ourselves."""
    pass


class SenderIsSelfError(Exception):
    """Raised when trying to send certain RPC commands, if sender is us."""
    pass


# class WithLock:
#     """
#     Lock object that can be used in "with" statements.
#     Example usage:
#         lock = threading.Lock()
#         with WithLock(lock):
#             do_stuff()
#         do_more_stuff()
#     Based from the following code:
#     https://www.bogotobogo.com/python/Multithread/python_multithreading_Synchronization_Lock_Objects_Acquire_Release.php
#     https://www.geeksforgeeks.org/with-statement-in-python/
#     """

#     def __init__(self, lock: Lock) -> None:
#         """
#         Creates lock object to be used in __enter__ and __exit__.
#         """
#         self.lock = lock

#     def __enter__(self) -> None:
#         """
#         Change the state to locked and returns immediately.
#         """
#         self.lock.acquire()

#     def __exit__(self, exc_type, exc_value, traceback) -> None:
#         """
#         Changes the state to unlocked; this is called from another thread.
#         """
#         self.lock.release()


@dataclass
class Constants:
    """
    https://xlattice.sourceforge.net/components/protocol/kademlia/specs.html

    A Kademlia network is characterized by three constants, which we call alpha, B, and k.
    The first and last are standard terms. The second is introduced because some Kademlia implementations use a
    different key length.

    alpha is a small number representing the degree of parallelism in network calls, usually 3
    B is the size in bits of the keys used to identify nodes and store and retrieve data; in basic Kademlia
    this is 160, the length of an SHA1 digest (hash)
    k is the maximum number of contacts stored in a bucket; this is normally 20
    It is also convenient to introduce several other constants not found in the original Kademlia papers.

    tExpire = 86400s, the time after which a key/value pair expires; this is a time-to-live (TTL) from the
    original publication date
    tRefresh = 3600s, after which an otherwise un-accessed bucket must be refreshed
    tReplicate = 3600s, the interval between Kademlia replication events, when a node is required to publish
    its entire database
    tRepublish = 86400s, the time after which the original publisher must republish a key/value pair
    """

    ORIGINATOR_REPUBLISH_INTERVAL: int  # TODO: Create.
    EVICTION_LIMIT: int  # TODO: create.
    K: int = 20
    B: int = 160
    A: int = 3
    EXPIRATION_TIME_SEC: int = 86400  # Seconds in a day.
    BUCKET_REFRESH_INTERVAL: int = 3600  # seconds in an hour.
    KEY_VALUE_REPUBLISH_INTERVAL: int = 86400  # Seconds in a day.

    DHT_SERIALISED_SUFFIX = "dht"


class ID:

    def __init__(self, value: int):
        """
        Kademlia node ID: This is an integer from 0 to 2^160 - 1

        Args:
            value: (int) ID denary value
        """

        self.MAX_ID = 2**160
        self.MIN_ID = 0
        if not (self.MAX_ID > value >= self.MIN_ID):
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
        Returns value in binary - this does not include a 0b tag at the start.
        """
        return bin(self.value)[2:]

    def big_endian_bytes(self) -> list[str]:
        """
        Returns the ID in big-endian binary - largest bit is at index 0.
        """
        big_endian = [x for x in self.bin()[2:]]
        return big_endian

    def little_endian_bytes(self) -> list[str]:
        """
        Returns the ID in little-endian binary - smallest bit is at index 0.
        """
        big_endian = [x for x in self.bin()[2:]][::-1]
        return big_endian

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
        return str(self.denary())

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
        return ID(2**159)

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
        return ID(bucket.low() + random.randint(0,
                                                bucket.high() - bucket.low()))

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


class IStorage:
    """Interface which 'abstracts the storage mechanism for key-value pairs.''"""

    @abstractmethod
    def contains(self, key: ID) -> bool:
        pass

    @abstractmethod
    def try_get_value(self, key: ID) -> tuple[bool, int | str]:
        pass

    @abstractmethod
    def get(self, key: ID | int) -> str:
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

    def __init__(self, id: ID, protocol=None):
        self.protocol: VirtualProtocol | IProtocol = protocol
        self.id = id
        self.last_seen: datetime = datetime.now()

    def touch(self) -> None:
        """Updates the last time the contact was seen."""
        self.last_seen = datetime.now()


class QueryReturn(TypedDict):
    """
    Has elements: contacts, val, found, found_by
    """
    contacts: list[Contact]
    val: str | None
    found: bool
    found_by: Contact | None


class Node:

    def __init__(self,
                 contact: Contact,
                 storage: IStorage,
                 cache_storage=None):

        self.our_contact: Contact = contact
        self._storage: IStorage = storage
        self.cache_storage = cache_storage
        self.DHT: DHT | None = None
        self.bucket_list = BucketList(contact.id)

    def ping(self, sender: Contact) -> Contact:
        # TODO: Complete.
        pass

    def store(self,
              key: ID,
              sender: Contact,
              val: str,
              is_cached: bool = False,
              expiration_time_sec: int = 0) -> None:

        if sender.id == self.our_contact.id:
            raise SenderIsSelfError("Sender should not be ourself.")

        # add sender to bucket_list (updating bucket list like how it is in spec.)
        self.bucket_list.add_contact(sender)

        if is_cached:
            self.cache_storage.set(key, val, expiration_time_sec)
        else:
            self.send_key_values_if_new_contact(sender)
            self._storage.set(key, val, Constants.EXPIRATION_TIME_SEC)

    def find_node(self, key: ID,
                  sender: Contact) -> tuple[list[Contact], str | None]:
        """
        Finds K close contacts to a given ID, while exluding the sender.
        It also adds the sender if it hasn't seen it before.
        :param key: K close contacts are found near this ID.
        :param sender: Contact to be excluded and added if new.
        :return: list of K (or less) contacts near the key
        """

        # managing sender
        if sender.id == self.our_contact.id:
            raise SendingQueryToSelfError("Sender cannot be ourselves.")
        self.send_key_values_if_new_contact(sender)
        self.bucket_list.add_contact(sender)

        # actually finding nodes
        # print([len(b.contacts) for b in self.bucket_list.buckets])
        contacts = self.bucket_list.get_close_contacts(key=key,
                                                       exclude=sender.id)
        # print(f"contacts: {contacts}")
        return contacts, None

    def find_value(self, key: ID, sender: Contact) \
            -> tuple[list[Contact] | None, str | None]:
        """
        Find value in self._storage, testing
        self.cache_storage if it is not in the former.
        If it is not in either, it gets K
        close contacts from the bucket list.
        """
        if sender.id == self.our_contact.id:
            raise SendingQueryToSelfError("Sender cannot be ourselves.")

        self.send_key_values_if_new_contact(sender)

        if self._storage.contains(key):
            return None, self._storage.get(key)
        elif self.cache_storage.contains(key):
            return None, self.cache_storage.get(key)
        else:
            return self.bucket_list.get_close_contacts(key, sender.id), None

    def send_key_values_if_new_contact(self, sender: Contact) -> None:
        """
        Spec: "When a new node joins the system, it must store any 
        key-value pair to which it is one of the k closest. Existing 
        nodes, by similarly exploiting complete knowledge of their 
        surrounding subtrees, will know which key-value pairs the new 
        node should store. Any node learning of a new node therefore 
        issues STORE RPCs to transfer relevant key-value pairs to the 
        new node. To avoid redundant STORE RPCs, however, a node only 
        transfers a key-value pair if it’s own ID is closer to the key 
        than are the IDs of other nodes."

        For a new contact, we store values to that contact whose keys 
        XOR our_contact are less than the stored keys XOR other_contacts.
        """
        if self._is_new_contact(sender):
            # with self.bucket_list.lock:
            # Clone so we can release the lock.
            contacts: list[Contact] = self.bucket_list.contacts()
            if len(contacts) > 0:
                # and our distance to the key < any other contact's distance
                # to the key
                for k in self._storage.get_keys():
                    # our minimum distance to the contact.
                    distance = min([c.id ^ k for c in contacts])
                    # If our contact is closer, store the contact on its
                    # node.
                    if (self.our_contact.id ^ k) < distance:
                        error: RPCError | None = sender.protocol.store(
                            sender=self.our_contact,
                            key=ID(k),
                            val=self._storage.get(k)
                        )
                        # if self.DHT: self.DHT.handle_error(error, sender)

    def _is_new_contact(self, sender: Contact) -> bool:
        ret: bool
        # with self.bucket_list.lock:
        ret: bool = self.bucket_list.contact_exists(sender)
        # end lock
        if self.DHT:  # might be None in unit testing
            # with self.DHT.pending_contacts.lock:
            ret |= (sender.id in [c.id for c in self.DHT.pending_contacts])
            # end lock

        return not ret
        
    def simply_store(self, key, val) -> None:
        """
        For unit testing.
        :param key:
        :param val:
        :return: None
        """
        self._storage.set(key, val)


class KBucket:

    def __init__(self,
                 initial_contacts: list[Contact] | None = None,
                 low: int = 0,
                 high: int = 2**160):
        """
        Initialises a k-bucket with a specific ID range, 
        initially from 0 to 2**160.
        """
        if initial_contacts is None:  # Fix for instead of setting initial_contacts = []
            initial_contacts = []

        self.contacts: list[Contact] = initial_contacts
        self._low = low
        self._high = high
        self.time_stamp: datetime = datetime.now()
        # self.lock = WithLock(Lock())

    def low(self):
        return self._low

    def high(self):
        return self._high

    def is_full(self) -> bool:
        """
        This INCLUDES K, so if there are 20 inside, no more can be added.
        :return: Boolean saying if it's full.
        """
        return len(self.contacts) >= Constants.K

    def contains(self, id: ID) -> bool:
        """
        Returns boolean determining whether a given contact ID is in the k-bucket.
        """

        # replaceable
        return any(id == contact.id for contact in self.contacts)

    def touch(self) -> None:
        self.time_stamp = datetime.now()

    def is_in_range(self, other_id: ID) -> bool:
        """
        Determines if a given ID is within the range of the k-bucket.
        :param other_id: The ID to be checked.
        :return: Boolean saying if it's in the range of the k-bucket.
        """
        return self._low <= other_id.value <= self._high

    def add_contact(self, contact: Contact):
        # TODO: Check if this is meant to check if it exists in the bucket.
        if self.is_full():
            raise TooManyContactsError(
                f"KBucket is full - (length is {len(self.contacts)}).")
        elif not self.is_in_range(contact.id):
            raise OutOfRangeError("Contact ID is out of range.")
        else:
            # !!! should this check if the contact is already in the bucket?
            self.contacts.append(contact)

    def depth(self) -> int:
        """
        "The depth is just the length of the prefix shared by all nodes in 
        the k-bucket’s range." Do not confuse that with this statement in the
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

            if len(a) < len(b):  # swap "a" and "b" if "a" is shorter
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
        # This doesn't work when all contacts are bunched towards one side of the KBucket.
        # It's in the spec so I'm keeping it, it also means it stays nice and neat

        midpoint: int = (self._low + self._high) // 2  # This will always be an integer, but // is faster than /.

        # Gets the median of all contacts inside the KBucket (rounding up in even # of contacts)
        # contact_ids_asc = sorted([c.id.value for c in self.contacts])
        # median_of_contact_id: int = median_high(contact_ids_asc)
        # midpoint = median_of_contact_id

        k1: KBucket = KBucket(low=self._low, high=midpoint)
        k2: KBucket = KBucket(low=midpoint, high=self._high)
        for c in self.contacts:
            if c.id.value < midpoint:
                k1.add_contact(c)
            else:
                k2.add_contact(c)

        return k1, k2

    def replace_contact(self, contact: Contact) -> None:
        """replaces contact, then touches it"""
        contact_ids = [c.id for c in self.contacts]
        index = contact_ids.index(contact.id)
        self.contacts[index] = contact
        contact.touch()


class BucketList:

    def __init__(self, our_id: ID):
        self.DHT = None
        self.buckets: list[KBucket] = [KBucket()]
        # first k-bucket has max range
        self.our_id: ID = our_id

        # create locking object
        # self.lock = WithLock(Lock())

    def can_split(self, kbucket: KBucket) -> bool:
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
        # with self.lock:
        # TODO: What is self.node?

        return (kbucket.is_in_range(self.our_id)
                or (kbucket.depth() % Constants.B != 0))

    def _get_kbucket_index(self, other_id: ID) -> int:
        """
        Returns the first k-buckets index in the bucket list
        which has a given ID in range. Returns -1 if not found.
        """

        # with self.lock:
        for i in range(len(self.buckets)):
            if self.buckets[i].is_in_range(other_id):
                return i
        return -1

    def get_kbucket(self, other_id: ID) -> KBucket:
        """
        Returns the first k-bucket in the bucket list
        which has a given ID in range. Raises an error if none are found
         - this should never happen!
        :param other_id: ID to used to determine range.
        :return: the first k-bucket which is in range.
        """

        try:
            bucket = self.buckets[self._get_kbucket_index(other_id)]
            return bucket

        except IndexError:
            raise OutOfRangeError(f"ID: {id} is not in range of bucket-list.")

    def add_contact(self, contact: Contact) -> None:
        """
        Adds a contact to a k-bucket in the list, this is determined by the range of k-buckets in the lists.
        This range should span the entire ID space - so there should always be a k-bucket to be added.

        This raises an error if we try to add ourselves to the k-bucket.

        :param contact: Contact to be added, this is touched in the process.
        :return: None
        """
        if self.our_id == contact.id:
            raise OurNodeCannotBeAContactError(
                "Cannot add ourselves as a contact.")

        contact.touch()  # Update the time last seen to now

        # with self.lock:
        kbucket: KBucket = self.get_kbucket(contact.id)
        if kbucket.contains(contact.id):
            print("Contact already in KBucket.")
            # replace contact, then touch it
            kbucket.replace_contact(contact)

        elif kbucket.is_full():
            if self.can_split(kbucket):
                # print("Splitting!")
                # Split then try again
                k1, k2 = kbucket.split()
                # print(f"K1: {len(k1.contacts)}, K2: {len(k2.contacts)}, Buckets: {self.buckets}")
                index: int = self._get_kbucket_index(contact.id)

                # adds the two buckets to 2 separate buckets.
                self.buckets[index] = k1  # Replaces original KBucket
                self.buckets.insert(index + 1, k2)  # Adds a new one after it
                # print(self.buckets)
                self.add_contact(
                    contact
                )  # Unless k <= 0, This should never cause a recursive loop

            else:
                last_seen_contact: Contact = sorted(
                    kbucket.contacts, key=lambda c: c.last_seen)[0]
                error: RPCError | None = last_seen_contact.protocol.ping(
                    self.our_contact)
                if error:
                    if self.DHT:  # tests may not initialise a DHT
                        self.DHT.delay_eviction(last_seen_contact, contact)
                else:
                    # still can't add the contact ,so put it into the pending list
                    if self.DHT:
                        self.DHT.add_to_pending(contact)

        else:
            # Bucket is not full, nothing special happens.
            kbucket.add_contact(contact)

    def get_close_contacts(self, key: ID, exclude: ID) -> list[Contact]:
        """
        Brute force distance lookup of all known contacts, sorted by distance.
        Then we take K of the closest.
        :param key: The ID for which we want to find close contacts.
        :param exclude: The ID to exclude (the requesters ID).
        :return: List of K contacts sorted by distance.
        """
        # print(key, exclude)
        # with self.lock:
        contacts = []
        # print(self.buckets)
        for bucket in self.buckets:
            # print(bucket.contacts)
            for contact in bucket.contacts:
                # print(contact.id.value)
                # print(f"Exclude: {exclude}")

                if contact.id != exclude:
                    contacts.append(contact)
        # print(contacts)
        contacts = sorted(contacts, key=lambda c: c.id ^ key)[:Constants.K]
        if len(contacts) > Constants.K and DEBUG:
            raise ValueError(
                f"Contacts should be smaller than or equal to K. Has length {len(contacts)}, "
                f"which is {Constants.K - len(contacts)} too big.")
        return contacts

    def contacts(self) -> list[Contact]:
        """
        Returns a list of all contacts in the bucket list.
        :return: All contacts in the bucket list.
        """
        contacts = []
        for bucket in self.buckets:
            for contact in bucket.contacts:
                contacts.append(contact)
        return contacts

    def contact_exists(self, contact: Contact) -> bool:
        return contact in self.contacts()


class Router:
    """
    TODO: Talk about what this does.
    """

    def __init__(self, node: Node = None) -> None:
        """
        TODO: what is self.node?
        :param node:
        """
        self.node: Node = node
        self.closer_contacts: list[Contact] = []
        self.further_contacts: list[Contact] = []
        # self.lock = WithLock(Lock())

    def lookup(self,
               key: ID,
               rpc_call: Callable,
               give_me_all: bool = False) -> QueryReturn:
        """
        Performs main Kademlia Lookup.
        :param key: Key to be looked up
        :param rpc_call: RPC call to be used.
        :param give_me_all: TODO: Implement.
        :return: returns query result.
        """
        have_work = True
        ret = []
        contacted_nodes = []
        # closer_uncontacted_nodes = []
        # further_uncontacted_nodes = []

        all_nodes = self.node.bucket_list.get_close_contacts(
            key, self.node.our_contact.id)[0:Constants.K]

        nodes_to_query: list[Contact] = all_nodes[0:Constants.A]

        for i in nodes_to_query:
            if i.id.value ^ key.value < self.node.our_contact.id.value ^ key.value:
                self.closer_contacts.append(i)
            else:
                self.further_contacts.append(i)

        # all untested contacts just get dumped here.
        for i in all_nodes[Constants.A + 1:]:
            self.further_contacts.append(i)

        for i in nodes_to_query:
            if i not in contacted_nodes:
                contacted_nodes.append(i)

        # In the spec they then send parallel async find_node RPC commands
        query_result: QueryReturn = (self.query(key, nodes_to_query, rpc_call,
                                                self.closer_contacts,
                                                self.further_contacts))

        if query_result["found"]:  # if a node responded
            return query_result

        # add any new closer contacts
        for i in self.closer_contacts:
            if i.id not in [j.id for j in ret
                            ]:  # if id does not already exist inside list
                ret.append(i)

        while len(ret) < Constants.K and have_work:
            closer_uncontacted_nodes = [
                i for i in self.closer_contacts if i not in contacted_nodes
            ]
            further_uncontacted_nodes = [
                i for i in self.further_contacts if i not in contacted_nodes
            ]

            # If we have uncontacted nodes, we still have work to be done.
            have_closer: bool = len(closer_uncontacted_nodes) > 0
            have_further: bool = len(further_uncontacted_nodes) > 0
            have_work: bool = have_closer or have_further
            """
            Spec: of the k nodes the initiator has heard of closest 
            to the target,
            it picks the 'a' that it has not yet queried and resends 
            the FIND_NODE RPC to them.
            """
            if have_closer:
                new_nodes_to_query = closer_uncontacted_nodes[:Constants.A]
                for i in new_nodes_to_query:
                    if i not in contacted_nodes:
                        contacted_nodes.append(i)

                query_result = (self.query(key, new_nodes_to_query, rpc_call,
                                           self.closer_contacts,
                                           self.further_contacts))

                if query_result["found"]:
                    return query_result

            elif have_further:
                new_nodes_to_query = further_uncontacted_nodes[:Constants.A]
                for i in new_nodes_to_query:
                    if i not in contacted_nodes:
                        contacted_nodes.append(i)

                query_result = (self.query(key, new_nodes_to_query, rpc_call,
                                           self.closer_contacts,
                                           self.further_contacts))

                if query_result["found"]:
                    return query_result

        # return k closer nodes sorted by distance,

        contacts = sorted(ret[:Constants.K], key=(lambda x: x.id ^ key))
        return {
            "found": False,
            "contacts": contacts,
            "val": None,
            "found_by": None
        }

    def find_closest_nonempty_kbucket(self, key: ID) -> KBucket:
        """
        Helper method.
        Code listing 34.
        """
        # gets all non-empty buckets from bucket list
        non_empty_buckets: list[KBucket] = [
            b for b in self.node.bucket_list.buckets if (len(b.contacts) != 0)
        ]
        if len(non_empty_buckets) == 0:
            raise AllKBucketsAreEmptyError(
                "No non-empty buckets can be found.")

        return sorted(non_empty_buckets,
                      key=(lambda b: b.id.value ^ key.value))[0]

    # TODO: Remove.
    """
    @staticmethod
    def get_closest_nodes(key: ID, bucket: KBucket) -> list[Contact]:
        return sorted(bucket.contacts, key=lambda c: c.id.value ^ key.value)
    """

    def rpc_find_nodes(self, key: ID, contact: Contact):
        # what is node??
        new_contacts, timeout_error = contact.protocol.find_node(
            self.node.our_contact, key)

        # dht?.handle_error(timeoutError, contact)

        return new_contacts, None, None

    def rpc_find_value(self, key, contact):
        # TODO: Create.
        pass

    def get_closer_nodes(self, key: ID, node_to_query: Contact,
                         rpc_call: Callable, closer_contacts: list[Contact],
                         further_contacts: list[Contact]) -> bool:

        contacts: list[Contact]
        found_by: Contact
        val: str
        contacts, found_by, val = rpc_call(key, node_to_query)
        peers_nodes = []
        for contact in contacts:
            if contact.id.value not in [
                    self.node.our_contact.id.value, node_to_query.id.value,
                    closer_contacts, further_contacts
            ]:
                peers_nodes.append(contact)

        nearest_node_distance = node_to_query.id.value ^ key.value

        # with self.lock:  # Lock thread while this is running.
        for p in peers_nodes:
            # if given nodes are closer than our nearest node
            # , and it hasn't already been added:
            if p.id.value ^ key.value < nearest_node_distance \
                    and p.id.value not in [i.id.value for i in closer_contacts]:
                closer_contacts.append(p)

        # with self.lock:  # Lock thread while this is running.
        for p in peers_nodes:
            # if given nodes are further than or equal to the nearest node
            # , and it hasn't already been added:
            if p.id.value ^ key.value >= nearest_node_distance \
                    and p.id.value not in [i.id.value for i in further_contacts]:
                further_contacts.append(p)

        return val is not None  # Can you use "is not" between empty strings and None?

    def query(self, key, new_nodes_to_query, rpc_call, closer_contacts,
              further_contacts) -> QueryReturn:
        pass


class IProtocol:
    # TODO: Create this!!
    # It shouldn't be too hard, it's just making skeletons
    # for type hinting all protocol methods.
    pass


class RPCError(Exception):
    """
    Errors for RPC commands.
    """

    @staticmethod
    def no_error():
        pass


class VirtualProtocol(IProtocol):
    """
    For unit testing, doesn't really do much
    """

    def __init__(self, node: Node | None = None, responds=True) -> None:
        self.responds = responds
        self.node = node

    def ping(self, sender: Contact) -> RPCError | None:
        if self.responds:
            self.node.ping(sender)
        else:
            return RPCError(
                "Time out while pinging contact - VirtualProtocol does not respond."
            )

    def find_node(self, sender: Contact,
                  key: ID) -> tuple[list[Contact], RPCError | None]:
        """
        Finds K close contacts to a given ID, while excluding the sender.
        It also adds the sender if it hasn't seen it before.
        :param key: K close contacts are found near this ID.
        :param sender: Contact to be excluded and added if new.
        :return: list of K (or less) contacts near the key, and an error that may need to be handled.
        """
        return self.node.find_node(sender=sender, key=key)[0], None

    def find_value(self, sender: Contact,
                   key: ID) -> tuple[list[Contact], str, RPCError | None]:
        """
        Returns either contacts or None if the value is found.
        """
        contacts, val = self.node.find_value(sender=sender, key=key)
        return contacts, val, None

    def store(self,
              sender: Contact,
              key: ID,
              val: str,
              is_cached=False,
              exp_time: int = 0) -> RPCError | None:
        """
        Stores the key-value on the remote peer.
        """
        self.node.store(sender=sender,
                        key=key,
                        val=val,
                        is_cached=is_cached,
                        expiration_time_sec=exp_time)

        return None


class VirtualStorage(IStorage):
    """
    Simple storage mechanism that stores things in memory.
    """

    def __init__(self):
        self._store: dict[int, str] = {}

    def contains(self, key: ID) -> bool:
        """
        Returns a boolean stating whether a key is storing something.
        """
        return key.value in list(self._store.keys())

    def get(self, key: ID | int) -> str:
        """
        Returns stored value, associated with given key value.
        :param key: Type ID or Integer, key value to be searched.
        :return:
        """
        if isinstance(type(key), ID):
            return self._store[key.value]
        elif isinstance(type(key), int):
            return self._store[key]
        else:
            raise TypeError("'get()' parameter 'key' must be type ID or int.")

    def get_timestamp(self, key: int) -> datetime:
        pass

    def set(self, key: ID, value: str, expiration_time_sec: int = 0) -> None:
        self._store[key.value] = value
        # TODO: Add expiration time to this.

    def get_expiration_time_sec(self, key: int) -> int:
        pass

    def remove(self, key: int) -> None:
        pass

    def get_keys(self) -> list[int]:
        pass

    def touch(self, key: int) -> None:
        pass

    def try_get_value(self, key: ID) -> tuple[bool, int | str]:
        pass


class DHT:
    """
    This is the main entry point for our peer to interact with other peers.

    This has multiple purposes:
     - One is to propagate key-values to other close peers on the network using a lookup algorithm.
     - Another is to use the same lookup algorithm to search for other close nodes that might have a value that we don’t have.
     - It is also used for bootstrapping our peer into a pre-existing network.

    """

    def __init__(self,
                 id: ID,
                 protocol: IProtocol,
                 router: Router,
                 storage_factory: Callable[[], IStorage] | None = None,
                 originator_storage: IStorage | None = None,
                 republish_storage: IStorage | None = None,
                 cache_storage: IStorage | None = None):
        """

        We use a wrapper Dht class, which will become the main entry point for our peer,
        for interacting with other peers. The purposes of this class are:

         - When storing a value, use the lookup algorithm to find other closer peers to 
         propagate the key-value.
         - When looking up a value, if our peer doesn’t have the value, we again use the
         lookup algorithm to find other closer nodes that might have the value.
         - A bootstrapping method that registers our peer with another peer and 
         initializes our bucket list with that peer’s closest contacts.
         
        Supports different concrete storage types.
        For example, you may want the cache_storage to be an in-memory store,
        the originator_storage to be a SQL database, and the republish store to be a 
        key-value database.
        
        :param id: ID associated with the DHT. TODO: More info.
        
        :param protocol: Protocol used by the DHT. TODO: More info - I'm not even sure 
        if this is correct.
        
        :param storage_factory: Storage to be used for all storage mechanisms - 
        if specific mechanisms are not provided.
        
        :param originator_storage: Pre-existing storage object to be used for main 
        storage. TODO: Is this right?
        
        :param republish_storage: This contains key-values that have been republished 
        by other peers. TODO: Is this right?
        
        :param cache_storage: Short term storage.
        
        :param router: Router object associated with the DHT. TODO: Is this right?
        """

        if originator_storage:
            self._originator_storage = originator_storage
        elif storage_factory:
            self._originator_storage = storage_factory()
        else:
            raise TypeError(
                "Originator storage must take parameter originator_storage,"
                " or be generated by generated by parameter storage_factory.")

        if republish_storage:
            self._republish_storage = republish_storage
        elif storage_factory:
            self._republish_storage = storage_factory()
        else:
            raise TypeError(
                "Republish storage must take parameter republish_storage,"
                " or be generated by generated by parameter storage_factory.")

        if cache_storage:
            self._cache_storage = cache_storage
        elif storage_factory:
            self._cache_storage = storage_factory()
        else:
            raise TypeError(
                "Cache storage must take parameter cache_storage,"
                " or be generated by generated by parameter storage_factory.")

        self.pending_contacts: list[Contact] = []
        self.our_id = id
        self.our_contact = Contact(id=id, protocol=protocol)
        self.node = Node(self.our_contact, storage=VirtualStorage())
        self.node.DHT = self
        self.node.bucket_list.DHT = self
        self._protocol = protocol
        self._router: Router = router
        self._router.node = self.node
        self._router.DHT = self

    def router(self) -> Router:
        return self._router

    def protocol(self) -> IProtocol:
        return self._protocol

    def originator_storage(self) -> IStorage:
        return self._originator_storage

    def store(self, key: ID, val: str) -> None:
        self.touch_bucket_with_key(key)
        # We're storing to K closer contacts
        self._originator_storage.set(key, val)
        self.store_on_closer_contacts(key, val)

    def find_value(self, key: ID) -> tuple[bool, list[Contact] | None, str | None]:
        """
        Attempts to find a given value.
        First it checks our originator storage. If the given key does not have a value in our storage,
        it will use Router.lookup() to attempt to find it. If there is no value found from router.lookup(), the value
        returned will be None.
        If there is a value found from router.lookup(), the value will be stored on the closest contact to us, if
        one exists.
        :param key: Key to search for value pair.
        :return: Found: bool (If it is found or not), contacts: list[Contact], val: str | None (value returned
        """
        self.touch_bucket_with_key(key)
        contacts_queried: list[Contact] = []

        # ret (found: False, contacts: None, val: None)
        found: bool = False
        contacts: list[Contact] | None = None  
        # TODO: This is never called again?? 
        # - Add to docstring when finished
        val: str | None = None

        # TODO: Talk about what this does - I haven't made it yet so IDK.
        our_val: str | None = self._originator_storage.try_get_value(key)[1]
        if our_val:
            found = True
            val = our_val
        else:
            lookup: QueryReturn = self._router.lookup(
                key, self._router.rpc_find_value)
            if lookup["found"]:
                found = True
                contacts = None
                val = lookup["val"]
                # Find the closest contact (other than the one the value was found by)
                # in which to "cache" the key-value.
                store_to: Contact | None = [
                    c for c in lookup["contacts"] 
                    if c != lookup["found_by"]
                ][0]

                if store_to:
                    separating_nodes: int = self._get_separating_nodes_count(self.our_contact, store_to)
                    exp_time_sec: int = int(
                        Constants.EXPIRATION_TIME_SEC / (2 ** separating_nodes)
                    )
                    error: RPCError = store_to.protocol.store(self.node.our_contact, key, lookup["val"])
                    # self.handle_error(error, store_to)

        return found, contacts, val

    def touch_bucket_with_key(self, key: ID) -> None:
        """
        Touches a KBucket with a given key from the bucket list.
        :return: Returns nothing.
        """
        self.node.bucket_list.get_kbucket(key).touch()

    def store_on_closer_contacts(self, key: ID, val: str) -> None:
        now: datetime = datetime.now()
        kbucket: KBucket = self.node.bucket_list.get_kbucket(key)
        contacts: list[Contact]
        if (now - kbucket.time_stamp) < timedelta(
                seconds=Constants.BUCKET_REFRESH_INTERVAL):
            # Bucket has been refreshed recently, so don't do a lookup as we
            # have the k closest contacts.
            contacts: list[Contact] = self.node.bucket_list.get_close_contacts(
                key=key, exclude=self.node.our_contact.id)
        else:
            contacts: list[Contact] = self._router.lookup(
                key, self._router.rpc_find_nodes)["contacts"]

        for c in contacts:
            error: RPCError | None = c.protocol.store(
                sender=self.node.our_contact, key=key, val=val)
            # handle_error(error, c)

    def bootstrap(self, known_peer: Contact) -> None:
        """
        This is how we join the network.

        We bootstrap our peer by contacting a known peer in the network, adding its contacts
        to our list, then getting the contacts for other peers not in the
        bucket range of our known peer we're joining.
        :param known_peer: Peer we know / are bootstrapping from.
        :return: RPC Error, not sure when it should be raised?
        """
        # print(f"Adding known peer with ID {known_peer.id}")
        self.node.bucket_list.add_contact(known_peer)

        # UNITTEST NOTES: This should return something in test_bootstrap_outside_bootstrapping_bucket,
        # it isn't at the moment.
        # find_node() should return the bucket list with the contact who knows 10 other contacts
        # it does.

        # finds K close contacts to self.our_id, excluding self.our_contact
        contacts, error = known_peer.protocol.find_node(
            sender=self.our_contact, key=self.our_id)
        # handle_error(error, known_peer)
        if not error:
            # print("NO ERROR")

            # add all contacts the known peer DIRECTLY knows
            for contact in contacts:
                self.node.bucket_list.add_contact(contact)

            known_peers_bucket: KBucket = self.node.bucket_list.get_kbucket(
                known_peer.id)

            if ID.max() in [c.id for c in known_peers_bucket.contacts]:
                print("somethings gone wrong")
            # Resolve the list now, so we don't include additional contacts
            # as we add to our bucket additional contacts.
            other_buckets: list[KBucket] = [
                i for i in self.node.bucket_list.buckets
                if i != known_peers_bucket
            ]
            for other_bucket in other_buckets:
                self._refresh_bucket(
                    other_bucket
                )  # UNITTEST Notes: one of these should contain the correct contact
        else:
            raise error

    def _refresh_bucket(self, bucket: KBucket) -> None:
        """
        Refreshes the given Kademlia KBucket by updating its last-touch timestamp,
        obtaining a random ID within the bucket's range, and attempting to find
        nodes in the network with that random ID.

        The method touches the bucket to update its last-touch timestamp, generates
        a random ID within the bucket's range, and queries nodes in the network
        using the Kademlia protocol to find nodes with the generated ID. If successful,
        the discovered contacts are added to the Kademlia node's bucket list.

        Note:
        The contacts collection for the given bucket might change during the operation,
        so it is isolated in a separate list before iterating over it.

        :param bucket: The KBucket to be refreshed.
        :returns: Nothing.
        """
        bucket.touch()
        random_id: ID = ID.random_id_within_bucket_range(bucket)

        # put in a separate list as contacts collection for this bucket might change.
        contacts: list[Contact] = bucket.contacts
        for contact in contacts:
            # print(contact.id, contact.protocol.node.bucket_list.contacts())
            new_contacts, timeout_error = contact.protocol.find_node(
                self.our_contact, random_id)
            # print(contacts.index(contact) + 1, "new contacts", new_contacts)
            # handle_error(timeout_error, contact)
            if new_contacts:
                for other_contact in new_contacts:
                    self.node.bucket_list.add_contact(other_contact)

    def _setup_bucket_refresh_timer(self) -> None:
        """
        Sets up the refresh timer to re-ping KBuckets.

        From the spec:
        “Buckets are generally kept fresh by the traffic of requests traveling through nodes. To handle pathological cases in which there are no lookups for a particular ID range, each node refreshes any bucket to which it has not performed a node lookup in the past hour. Refreshing means picking a random ID in the bucket’s range and performing a node search for that ID.”
        """
        bucket_refresh_timer = Timer(Constants.BUCKET_REFRESH_INTERVAL)
        bucket_refresh_timer.auto_reset = True
        bucket_refresh_timer.elapsed += self.bucket_refresh_timer_elapsed
        bucket_refresh_timer.start()

    def _bucket_refresh_timer_elapsed(self, sender: object, e):
        now: datetime = datetime.now()
        # Put into a separate list as bucket collections may be modified.
        current_buckets: list[KBucket] = [
            b for b in self.node.bucket_list.buckets
            if (now -
                b.time_stamp).seconds >= Constants.BUCKET_REFRESH_INTERVAL
        ]

        for b in current_buckets:
            self._refresh_bucket(b)

    def _key_value_republish_elapsed(self, sender: object, e) -> None:
        """
        Replicate key values if the key value hasn't been touched within 
        the republish interval. Also don't do a FindNode lookup if the
        bucket containing the key has been refresed within the refresh 
        interval.
        """
        now: datetime = datetime.now()

        rep_keys = [
            k for k in self._republish_storage.get_keys()
            if now - self._republish_storage.get_timestamp(k) >=
            Constants.KEY_VALUE_REPUBLISH_INTERVAL
        ]

        for k in rep_keys:
            key: ID = ID(k)
            # TODO: fix
            self.store_on_closer_contacts(key,
                                          self._republish_storage.get(key))
            self._republish_storage.touch(k)

    def _expire_keys_elapsed(self, sender: object, e) -> None:
        """
        Expired key-values are removed from the republish and
        cache storage.
        """
        self._remove_expired_data(self._cache_storage)
        self._remove_expired_data(self._republish_storage)

    @staticmethod
    def _remove_expired_data(store: IStorage) -> None:
        now: datetime = datetime.now()
        # to list so our key list is resolved now as we remove keys
        expired: list[int] = [
            key for key in store.get_keys()
            if (now - store.get_timestamp(key)) >= timedelta(
                seconds=store.get_expiration_time_sec(key))
        ]

        # expired is a list of all expired keys in the given storage.
        for key in expired:
            store.remove(key)

    def _originator_republish_elapsed(self, sender: object, e) -> None:
        """
        Spec: “For Kademlia’s current application (file sharing), 
        we also require the original publisher of a (key,value) 
        pair to republish it every 24 hours. Otherwise, (key,value) 
        pairs expire 24 hours after publication, so as to limit stale 
        index information in the system. For other applications, such 
        as digital certificates or cryptographic hash to value mappings, 
        longer expiration times may be appropriate.”
        """
        now: datetime = datetime.now()

        keys_pending_republish = [
            key for key in self._originator_storage.get_keys()
            if (now -
                self._originator_storage.get_timestamp(key)) >= timedelta(
                    seconds=Constants.ORIGINATOR_REPUBLISH_INTERVAL)
        ]

        for k in keys_pending_republish:
            key: ID = ID(k)
            # Just use close contacts, don't do a lookup TODO: why?
            contacts = self.node.bucket_list.get_close_contacts(
                key, self.node.our_contact.id)

            for c in contacts:
                error: RPCError | None = c.protocol.store(
                    sender=self.our_contact,
                    key=key,
                    val=self._originator_storage.get(key)
                )
                # handle_error(error, c)

            self._originator_storage.touch(k)

    def _get_separating_nodes_count(self, our_contact, store_to):
        pass

    def save(self, filename: str) -> None:
        """
        Saves DHT to file.
        """
        with open(filename, "wb") as output_file:
            pickle.dump(self, file=output_file)

    @classmethod
    def load(cls, filename):
        """
        Loads DHT from file.
        """
        with open(filename, "rb") as input_file:
            return pickle.load(file=input_file)


class BaseRequest:

    def __init__(self):
        self.protocol: object
        self.protocol_name: str
        self.random_id = ID.random_id().value
        self.sender: int


class FindNodeRequest(BaseRequest):
    def __init__(self):
        super().__init__()
        self.key: int


class FindValueRequest(BaseRequest):
    def __init__(self):
        super().__init__()
        self.key: int


class PingRequest(BaseRequest):
    pass


class StoreRequest(BaseRequest):
    def __init__(self):
        super().__init__()
        self.key: int
        self.value: str
        self.is_cached: bool
        self.expiration_time_sec: int


class ITCPSubnet:
    """
    Interface used for TCP.
    """
    def __init__(self):
        self.subnet: int


class FindNodeSubnetRequest(FindNodeRequest, ITCPSubnet):
    def __init__(self):
        super().__init__()
        self.subnet: int


class FindValueSubnetRequest(FindValueRequest, ITCPSubnet):
    def __init__(self):
        super().__init__()
        self.subnet: int


class PingSubnetRequest(PingRequest, ITCPSubnet):
    def __init__(self):
        super().__init__()
        self.subnet: int


class StoreSubnetRequest(StoreRequest, ITCPSubnet):
    def __init__(self):
        super().__init__()
        self.subnet: int



# class DHTSubclass(DHT):
#     def __init__(self):
#         super().__init__()
#
#     # @override
#     def expire_keys_elapsed(self, sender: object, e) -> None:
#         """
#         Allows for never expiring republished key values.
#         """
#         self.remove_expired_data(self.cache_storage)
#         # self.remove_expired_data(self.republish_storage)


def empty_node():
    """
    For testing.
    :return:
    """
    return Node(Contact(id=ID(0)), storage=VirtualStorage())


def random_node():
    return Node(Contact(id=ID.random_id()), storage=VirtualStorage())


def select_random(arr: list, freq: int) -> list:
    return random.sample(arr, freq)
