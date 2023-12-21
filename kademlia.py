import random
from abc import abstractmethod
from datetime import datetime
from threading import Lock

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


class WithLock:
    """
    Lock object that can be used in "with" statements.

    Example usage:

        lock = threading.Lock()

        with WithLock(lock):
            do_stuff()

        do_more_stuff()

    Based from the following code:

    https://www.bogotobogo.com/python/Multithread/python_multithreading_Synchronization_Lock_Objects_Acquire_Release.php

    https://www.geeksforgeeks.org/with-statement-in-python/

    """

    def __init__(self, lock: Lock) -> None:
        """
        Creates lock object to be used in __enter__ and __exit__.
        """
        self.lock = lock

    def __enter__(self) -> None:
        """
        Change the state to locked and returns immediately.
        """
        self.lock.acquire()

    def __exit__(self) -> None:
        """
        Changes the state to unlocked; this is called from another thread.
        """
        self.lock.release()


class Constants:

    def __init__(self):
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
        self.K = 20
        self.B = 160
        self.A = 10
        self.EXPIRATION_TIME_SEC = 86400  # TODO: Give this a proper number.


class ID:

    def __init__(self, value: int):
        """
        Kademlia node ID: This is an integer from 0 to 2^160 - 1

        Args:
            value: (int) ID denary value
        """

        self.MAX_ID = 2 ** 160
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
        Returns value in binary - this includes a 0b tag at the start.
        Do [2:] to remove this.
        """
        return bin(self.value)

    def big_endian_bytes(self) -> list:
        """
        Returns the ID in big-endian binary - largest bit is at index 0.
        """
        big_endian = [x for x in self.bin()[2:]]
        return big_endian

    def little_endian_bytes(self) -> list:
        """
        Returns the ID in little-endian binary - smallest bit is at index 0.
        """
        big_endian = [x for x in self.bin()[2:]][::-1]
        return big_endian

    def __xor__(self, val) -> int:
        if type(val) == ID:
            return self.value ^ val.value
        else:
            return self.value ^ val

    def __eq__(self, val) -> bool:
        if type(val) == ID:
            return self.value == val.value
        else:
            return self.value == val

    def __ge__(self, val):
        if type(val) == ID:
            return self.value >= val.value
        else:
            return self.value >= val

    def __le__(self, val):
        if type(val) == ID:
            return self.value <= val.value
        else:
            return self.value <= val

    def __lt__(self, val):
        if type(val) == ID:
            return self.value < val.value
        else:
            return self.value < val

    def __gt__(self, val):
        if type(val) == ID:
            return self.value > val.value
        else:
            return self.value > val

    def __str__(self) -> str:
        return str(self.denary())


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

        self.our_contact: Contact = contact
        self._storage: IStorage = storage
        self.cache_storage = cache_storage
        self.DHT: DHT
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

        # add sender to bucket_list (updating bucket list like is in spec.)
        self.bucket_list.add_contact(sender)

        if is_cached:
            self.cache_storage.set(key, val, expiration_time_sec)
        else:
            self.send_key_values_if_new_contact(sender)
            self._storage.set(key, val, Constants().EXPIRATION_TIME_SEC)

    def find_node(self, key: ID, sender: Contact) -> tuple[list[Contact], str | None]:
        if sender.id == self.our_contact.id:
            raise SendingQueryToSelfError("Sender cannot be ourselves.")

        self.send_key_values_if_new_contact(sender)

        self.bucket_list.add_contact(sender)
        contacts = self.bucket_list.get_close_contacts(key=key, exclude=sender.id)

        return contacts, None

    def find_value(self, key: ID, sender: Contact) \
            -> tuple[list[Contact] | None, str | None]:
        if sender.id == self.our_contact.id:
            raise SendingQueryToSelfError("Sender cannot be ourselves.")

        self.send_key_values_if_new_contact(sender)

        if self._storage.contains(key):
            return None, self._storage.get(key)
        elif self.cache_storage.contains(key):
            return None, self.cache_storage.get(key)
        else:
            return self.bucket_list.get_close_contacts(key, sender.id), None

    def send_key_values_if_new_contact(self, sender: Contact):
        pass


class KBucket:

    def __init__(self, initial_contacts: list[Contact] = [], low: int = 0, high: int = 2 ** 160):
        """
        Initialises a k-bucket with a specific ID range, 
        initially from 0 to 2**160.
        """
        self.contacts: list[Contact] = initial_contacts
        self._low = low
        self._high = high
        self.time_stamp: datetime = datetime.now()
        self.lock = Lock()

    def is_full(self) -> bool:
        return len(self.contacts) >= Constants().K

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
            raise TooManyContactsError("KBucket is full.")
        elif not self.is_in_range(contact.id):
            raise OutOfRangeError("Contact ID is out of range.")
        else:
            # !!! should this check if the contact is already in the bucket?
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

            return (self.is_in_range(self.node.id)
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
            if c.id.value < midpoint:
                k1.add_contact(c)
            else:
                k2.add_contact(c)

        return k1, k2

    def replace_contact(self, contact):
        pass


class BucketList:

    def __init__(self, our_id: ID):
        self.buckets: list[KBucket] = [KBucket()]
        # first k-bucket has max range
        self.our_id: ID = our_id

        # create locking object
        self.lock = Lock()

        # DHT object?
        self.DHT: DHT

    def _get_kbucket_index(self, other_id: ID) -> int:
        """
        Returns the first k-buckets index in the bucket list
        which has a given ID in range. Returns -1 if not found.
        """

        with self.lock:
            for i in range(len(self.buckets)):
                if self.buckets[i].is_in_range(other_id):
                    return i
            return -1

    def get_kbucket(self, other_id: ID) -> KBucket:
        """
        Returns the first k-bucket in the bucket list
        which has a given ID in range. Raises an error if none are found
         - this should never happen!
        :param other_id:  ID to used to determine range.
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

        TODO: Talk about locking.

        :param contact: Contact to be added, this is touched in the process.
        :return: None
        """
        if self.our_id == contact.id:
            raise OurNodeCannotBeAContactError(
                "Cannot add ourselves as a contact.")

        contact.touch()  # Update the time last seen to now

        with self.lock:
            kbucket: KBucket = self.get_kbucket(contact.id)
            if kbucket.contains(contact.id):
                # replace contact, then touch it
                kbucket.replace_contact(contact)

            elif kbucket.is_full():

                if kbucket.can_split():
                    # Split then try again
                    k1, k2 = kbucket.split()
                    index: int = self._get_kbucket_index(contact.id)

                    # adds the two buckets to 2 separate buckets.
                    self.buckets[index] = k1
                    self.buckets.insert(index + 1, k2)
                    self.add_contact(contact)  # Unless k <= 0, This should never cause a recursive loop

                else:
                    # TODO: Ping the oldest contact to see if it's still around and replace it if not.
                    pass

            else:
                # Bucket is not full, nothing special happens.
                kbucket.add_contact(contact)

    def get_close_contacts(self, key: ID, exclude: ID) -> list[Contact]:
        """
        TODO: Is this in the right class (Code listing 42)
        Brute force distance lookup of all known contacts, sorted by distance.
        Then we take K of the closeset.
        :param key: The ID for which we want to find close contacts.
        :param exclude: The ID to exclude (the requestor's ID).
        :return: List of K contacts sorted by distance.
        """

        def get_distance(contact: Contact):
            return contact.id.value ^ key.value

        with self.lock:
            contacts = []
            count = 0
            for bucket in self.buckets:
                for contact in bucket.contacts:
                    if count < Constants().K:  # Should get K items
                        if contact.id != exclude:
                            count += 1
                            contacts.append(contact)
                    else:
                        break
                        # More efficient that comparing count tons.
            if len(contacts) > Constants().K and DEBUG:
                raise ValueError(
                    f"Contacts should be smaller than or equal to K. Has length {len.contacts}, which is {Constants().K - len.contacts} too big.")
            return sorted(contacts, key=get_distance)


class Router:
    """
    TODO: Talk about what this does.
    """

    def __init__(self, node: Node) -> None:
        """
        TODO: what is self.node?
        :param node:
        """
        self.node = node
        self.lock = WithLock(Lock())

    def lookup(self, key: ID, rpc_call, give_me_all: bool = False) -> tuple:
        have_work = True
        ret = []
        contacted_nodes = []
        closer_contacts: list[Contact] = []
        further_contacts: list[Contact] = []
        closer_uncontacted_nodes = []
        further_uncontacted_nodes = []

        all_nodes = self.node.bucket_list.get_close_contacts(key, self.node.our_contact.id)[0:Constants().K]

        nodes_to_query: list[Contact] = all_nodes[0:Constants().A]

        for i in nodes_to_query:
            if i.id.value ^ key.value < self.node.our_contact.id.value ^ key.value:
                closer_contacts.append(i)
            else:
                further_contacts.append(i)

        # all untested contacts just get dumped here.
        for i in all_nodes[Constants().A + 1:]:
            further_contacts.append(i)

        for i in nodes_to_query:
            if i not in contacted_nodes:
                contacted_nodes.append(i)

        # In the spec they then send parallel async find_node RPC commands
        query_result = Query(key, nodes_to_query, rpc_call, closer_contacts, further_contacts)

        if query_result.found:  # if a node responded
            return query_result

        # add any new closer contacts
        for i in closer_contacts:
            if i.id not in [j.id for j in ret]:  # if id does not already exist inside list
                ret.append(i)

        while len(ret) < Constants().K and have_work:
            closer_uncontacted_nodes = [i for i in closer_contacts if i not in contacted_nodes]
            further_uncontacted_nodes = [i for i in further_contacts if i not in contacted_nodes]

            # If we have uncontacted nodes, we still have work to be done.
            have_closer: bool = len(closer_uncontacted_nodes) > 0
            have_further: bool = len(further_uncontacted_nodes) > 0
            have_work = have_closer or have_further

            """
            Spec: of the k nodes the initiator has heard of closest to the target,
            it picks the 'a' that it has not yet queried and resends the FIND_NODE RPC
            to them.
            """
            if have_closer:
                new_nodes_to_query = closer_uncontacted_nodes[:Constants().A]
                for i in new_nodes_to_query:
                    if i not in contacted_nodes:
                        contacted_nodes.append(i)

                query_result = Query(key, new_nodes_to_query, rpc_call, closer_contacts, further_contacts)

                if query_result.found:
                    return query_result

            elif have_further:
                new_nodes_to_query = further_uncontacted_nodes[:Constants().A]
                for i in new_nodes_to_query:
                    if i not in contacted_nodes:
                        contacted_nodes.append(i)

                query_result = Query(key, new_nodes_to_query, rpc_call, closer_contacts, further_contacts)

                if query_result.found:
                    return query_result

        # return k closer nodes sorted by distance,

        contacts = sorted(ret[:Constants().K], key=(lambda x: x.id ^ key))
        return False, contacts, None, None

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
            raise AllKBucketsAreEmptyError("No non-empty buckets can be found.")

        return sorted(non_empty_buckets, key=(lambda b: b.id.value ^ key.value))[0]

    @staticmethod
    def get_closest_nodes(key: ID, bucket: KBucket) -> list[Contact]:
        return sorted(bucket.contacts, key=lambda c: c.id.value ^ key.value)

    def rpc_find_nodes(self, key: ID, contact: Contact):
        # what is node??
        new_contacts, timeout_error = contact.protocol.find_node(self.node.our_contact, key)

        # dht?.handle_error(timeoutError, contact)

        return new_contacts, None, None

    def get_closer_nodes(self, key: ID, node_to_query: Contact, rpc_call,
                         closer_contacts: list[Contact], further_contacts: list[Contact]) -> bool:

        contacts: list[Contact]
        found_by: Contact
        val: str
        contacts, found_by, val = rpc_call(key, node_to_query)
        peers_nodes = []
        for contact in contacts:
            if contact.id.value not in [self.node.our_contact.id.value,
                                        node_to_query.id.value,
                                        closer_contacts,
                                        further_contacts]:
                peers_nodes.append(contact)

        nearest_node_distance = node_to_query.id.value ^ key.value

        with self.lock:  # Lock thread while this is running.
            for p in peers_nodes:
                # if given nodes are closer than our nearest node
                # , and it hasn't already been added:
                if p.id.value ^ key.value < nearest_node_distance \
                        and p.id.value not in [i.id.value for i in closer_contacts]:
                    closer_contacts.append(p)

        with self.lock:  # Lock thread while this is running.
            for p in peers_nodes:
                # if given nodes are further than or equal to the nearest node
                # , and it hasn't already been added:
                if p.id.value ^ key.value >= nearest_node_distance \
                        and p.id.value not in [i.id.value for i in further_contacts]:
                    further_contacts.append(p)

        return val is not None  # Can you use "is not" between empty strings and None?


class IProtocol:
    pass


class RPCError(Exception):
    """
    Errors for RPC commands.
    """

    @staticmethod
    def no_error():
        pass


class VirtualProtocol(IProtocol):  # TODO: what is IProtocol in code listing 40?
    """
    For unit testing, doesn't really do much
    """

    def __init__(self, node: Node | None = None, responds=True) -> None:
        self.responds = responds
        self.node = node

    @staticmethod
    def _NoError() -> RPCError:
        return RPCError()

    def ping(self, sender: Contact) -> RPCError:
        return RPCError()

    def find_node(self, sender: Contact, key: ID) -> tuple[list[Contact], RPCError]:
        """
        Get the list of contacts for this node closest to the key.
        """
        return self.node.find_node(sender=sender, key=key)[0], self._NoError()

    def find_value(self, sender: Contact, key: ID) -> tuple[list[Contact], str, RPCError]:
        """
        Returns either contacts or None if the value is found.
        """
        contacts, val = self.node.find_value(sender, key)
        return contacts, val, self._NoError()

    def store(self,
              sender: Contact,
              key: ID,
              val: str,
              is_cached=False,
              exp_time: int = 0) -> RPCError:
        """
        Stores the key-value on the remote peer.
        """
        self.node.store(sender=sender,
                        key=key,
                        val=val,
                        is_cached=is_cached,
                        expiration_time_sec=exp_time)

        return self._NoError()


class VirtualStorage(IStorage):
    """
    Simple storage mechanism that stores things in memory.
    """

    def __init__(self):
        self._store: dict = {}

    def contains(self, key: ID) -> bool:
        """
        Returns a boolean stating whether a key is storing something.
        """
        return key.value in list(self._store.keys())

    def get(self, key):
        """
        Returns stored value, associated with given key value.
        :param key: Type ID or Integer, key value to be searched.
        :return:
        """
        if type(key) == ID:
            return self._store[key.value]
        elif type(key) == int:
            return self._store[key]
        else:
            raise TypeError("'get()' parameter 'key' must be type ID or int.")


class DHT:

    def __init__(self,
                 id: ID,
                 protocol: IProtocol,
                 router: Router):
        self._router = None
        self._originator_storage = self.storage_factory()
        self.our_id = id
        self.our_contact = Contact(contact_ID=id, protocol=protocol)
        self._node = Node(self.our_contact, storage=VirtualStorage())
        self._node.DHT = self
        self._node.bucket_list.DHT = self
        self._protocol = protocol
        self._router = router
        self._router.node = self._node
        self._router.DHT = self

    def router(self):
        return self._router

    def protocol(self):
        return self._protocol

    def originator_storage(self):
        return self._originator_storage

    def store(self, key: ID, val: str) -> None:
        self.touch_bucket_with_key(key)
        # We're storing to K closer contacts
        self._originator_storage.set(key, val)
        self.store_on_closer_contacts(key, val)

    def find_value(self, key: id) -> tuple[bool, list[Contact], str]:
        self.touch_bucket_with_key(key)
        our_val: str


def random_id_in_space(low=0, high=2 ** 160, seed=None):
    """
    FOR TESTING PURPOSES.
    TODO: Remove.
    Generating random ID's this way will not perfectly spread the prefixes,
    this is a maths law I've forgotten - due to the small scale of this 
    I don't particularly see the need to perfectly randomise this.

    If I do though, here's how it would be done:
    - Randomly generate each individual bit, then concatenate.
    """
    if seed:
        random.seed(seed)
    return ID(random.randint(low, high))


def empty_node():
    """
    For testing.
    :return:
    """
    return Node(Contact(contact_ID=ID(0)), storage=VirtualStorage())


def random_node():
    return Node(Contact(contact_ID=random_id_in_space()), storage=VirtualStorage())


def select_random(arr: list, freq: int) -> list:
    return random.sample(arr, freq)
