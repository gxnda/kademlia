import logging
import math
import os
import random
import shutil
import unittest

import ui_helpers
from kademlia_dht.buckets import BucketList, KBucket
from kademlia_dht.constants import Constants
from kademlia_dht.contact import Contact
from kademlia_dht.dht import DHT
from kademlia_dht.errors import RPCError, TooManyContactsError
from kademlia_dht.id import ID
from kademlia_dht.networking import TCPSubnetServer, TCPServer
from kademlia_dht.node import Node
from kademlia_dht.protocols import TCPSubnetProtocol, VirtualProtocol
from kademlia_dht.routers import ParallelRouter, Router
from kademlia_dht.storage import VirtualStorage, SecondaryJSONStorage

Constants.DEBUG = True

if Constants.DEBUG:
    random.seed(0)


logger = ui_helpers.create_logger(verbose=True)
logger.info("Starting unit tests.")

def setup_split_failure(bucket_list=None):
    # force host node ID to < 2 ** 159 so the node ID is not in the
    # 2 ** 159 ... 2 ** 160 range.

    # b_host_id = bytearray()
    # b_host_id.extend([0] * 20)
    # b_host_id[19] = 0x7F

    # May be incorrect - book does some weird byte manipulation.
    host_id: ID = ID.random_id(2 ** 158, 2 ** 159 - 1)

    dummy_contact: Contact = Contact(host_id, VirtualProtocol())
    dummy_contact.protocol.node = Node(dummy_contact, VirtualStorage())

    if not bucket_list:
        bucket_list = BucketList(our_contact=dummy_contact)
        bucket_list.our_id = host_id

    # Also add a contact in this 0 - 2 ** 159 range
    # This ensures that only 1 bucket split will occur after 20 nodes with ID >= 2 ** 159 are added.
    dummy_contact = Contact(ID(1), VirtualProtocol())
    dummy_contact.protocol.node = Node(dummy_contact, VirtualStorage())
    bucket_list.add_contact(Contact(ID(1), dummy_contact.protocol))

    assert len(bucket_list.buckets) == 1  # Bucket split should not have occurred.
    assert len(bucket_list.buckets[0].contacts) == 1  # Expected 1 contact in bucket 0.

    # Make sure contact IDs all have the same 5-bit prefix and are 
    # in the 2 ** 159 ... 2 ** 160 - 1 space.

    b_contact_id: bytearray = bytearray()
    b_contact_id.extend([0] * 20)
    b_contact_id[19] = 0x80

    # 1000 xxxx prefix, xxxx starts at 1000 (8)
    # this ensures that all the contacts in a bucket match only the  
    # prefix as only the first 5 bits are shared.
    # |----| shared range
    # 1000 1000 ...
    # 1000 1100 ...
    # 1000 1110 ...
    shifter = 0x08
    pos: int = 19

    for _ in range(Constants.K):
        b_contact_id[pos] |= shifter  # |= is Bitwise OR.
        contact_id: ID = ID(
            int.from_bytes(b_contact_id, byteorder="little")
        )
        dummy_contact = Contact(ID(1), VirtualProtocol())
        dummy_contact.protocol.node = Node(dummy_contact, VirtualStorage())
        bucket_list.add_contact(
            Contact(contact_id, dummy_contact.protocol)
        )
        shifter >>= 1  # Right shift (Halves the shift value.)
        if shifter == 0:
            shifter = 0x80
            pos -= 1
    return bucket_list


class KBucketTest(unittest.TestCase):

    def test_add_to_kbucket(self):
        """
        Description
        Contact added to a full bucket.

        Expected
        “TooManyContactsError” should be raised.

        :return:
        """
        k = Constants.K
        k_bucket = KBucket()
        for i in range(k):
            contact = Contact(ID(i))
            k_bucket.add_contact(contact)
        self.assertTrue(len(k_bucket.contacts) == k)

    def test_too_many_contacts(self):
        """
        Description
        Contact added to an almost full bucket.

        Expected
        No exceptions should be raised, length of bucket contacts should be Constants.K

        Notes
        This implies standard behaviour of contacts being added to a relatively empty bucket,
        as it is required as a prerequisite to this test.
        :return:
        """

        k = Constants.K
        k_bucket = KBucket()
        for i in range(k):
            contact = Contact(ID(i))
            k_bucket.add_contact(contact)
        with self.assertRaises(TooManyContactsError):
            # Trying to add one more contact should raise the exception
            contact = Contact(ID(k + 1))
            k_bucket.add_contact(contact)

    def test_no_funny_business(self):
        """
        Description
        Compare a Kbucket with no initial contacts parameter, to one
        with an empty initial contacts parameter

        Expected
        They are the same.

        :return:
        """
        k1: KBucket = KBucket(low=0, high=100)
        k2: KBucket = KBucket(low=10, high=200, initial_contacts=[])
        self.assertTrue(k1.contacts == k2.contacts)


class AddContactTest(unittest.TestCase):

    def test_unique_id_add(self):
        """
        Description

        Adding K contacts to bucket list.

        Expected

        Bucket list should not split into separate buckets, and K contacts should exist in one bucket.
        :return:
        """
        dummy_contact: Contact = Contact(id=ID(0),
                                         protocol=VirtualProtocol())

        dummy_contact.protocol.node = Node(dummy_contact, VirtualStorage())

        bucket_list: BucketList = BucketList(dummy_contact)
        bucket_list.our_id = ID.random_id()

        for i in range(Constants.K):
            bucket_list.add_contact(Contact(ID.random_id()))

        self.assertTrue(
            len(bucket_list.buckets) == 1, "No split should have taken place.")

        self.assertTrue(
            len(bucket_list.buckets[0].contacts) == Constants.K,
            "K contacts should have been added.")

    def test_duplicate_id(self):
        """
        Description

        Adding 1 contact to a bucket list twice.

        Expected
        The bucket list should realise that the contact ID already exists in the buckets,
        therefore it should not be added.
        :return:
        """
        dummy_contact = Contact(ID(0), VirtualProtocol())
        dummy_contact.protocol.node = Node(dummy_contact, VirtualStorage())
        bucket_list: BucketList = BucketList(dummy_contact)
        bucket_list.our_id = ID.random_id()

        id: ID = ID.random_id()

        bucket_list.add_contact(Contact(id))
        bucket_list.add_contact(Contact(id))

        self.assertTrue(
            len(bucket_list.buckets) == 1, "No split should have taken place.")

        self.assertTrue(
            len(bucket_list.buckets[0].contacts) == 1,
            "Bucket should have one contact.")

    def test_bucket_split(self):
        """
        Description

        Adding K + 1 contacts to an empty bucket list.

        Expected

        The bucket list should split into 2 separate buckets.


        :return:
        """

        dummy_contact = Contact(ID(0), VirtualProtocol())
        dummy_contact.protocol.node = Node(dummy_contact, VirtualStorage())
        bucket_list: BucketList = BucketList(dummy_contact)
        bucket_list.our_id = ID.random_id()
        for i in range(Constants.K):
            bucket_list.add_contact(Contact(ID.random_id()))
        bucket_list.add_contact(Contact(ID.random_id()))

        print(f"KBucket range for first bucket: {bucket_list.buckets[0].low()}, "
              f"{bucket_list.buckets[0].high()}, high log 2: {math.log(bucket_list.buckets[0].high(), 2)}")
        print(f"KBucket range for second bucket: {bucket_list.buckets[1].low()}, "
              f"{bucket_list.buckets[1].high()}, high log 2: {math.log(bucket_list.buckets[1].high(), 2)}")

        self.assertTrue(
            len(bucket_list.buckets) > 1,
            "Bucket should have split into two or more buckets. "
            f"Length of first buckets contacts = {len(bucket_list.buckets[0].contacts)}")


class ForceFailedAddTest(unittest.TestCase):
    def test_force_failed_add(self):
        """
        Description

        Creates a bucket list composed of K ID’s, with a depth of 5 in the range 2 ** 159 to 2 ** 160 – 1,
        along with another Contact with ID in range 0 to 2 ** 159 – 1.
        Then another contact should be added with ID >= 2 ** 159.

        Expected

        Bucket split should occur, with 1 contact in the first bucket, and 20 contacts in the second bucket.
        Then when the 22nd contact is added, nothing should have changed, due to the depth of the bucket it’s being
        added to MOD 5 is 0.
        :return:
        """
        dummy_contact = Contact(id=ID(0))
        node = Node(contact=dummy_contact, storage=VirtualStorage())

        bucket_list: BucketList = setup_split_failure()

        self.assertTrue(len(bucket_list.buckets) == 2,
                        f"Bucket split should have occurred. Number of buckets should be 2, is {len(bucket_list.buckets)}.")

        self.assertTrue(len(bucket_list.buckets[0].contacts) == 1,
                        "Expected 1 contact in bucket 0.")

        self.assertTrue(len(bucket_list.buckets[1].contacts) == 20,
                        "Expected 20 contacts in bucket 1.")

        # This next contact should not split the bucket as
        # depth == 5 and therefore adding the contact will fail.

        # Any unique ID >= 2^159 will do.

        id = 2 ** 159 + 4

        new_contact = Contact(id=ID(id),
                              protocol=dummy_contact.protocol)
        bucket_list.add_contact(new_contact)

        self.assertTrue(len(bucket_list.buckets) == 2,
                        f"Bucket split should have occured. Number of buckets should be 2, is {len(bucket_list.buckets)}.")

        self.assertTrue(len(bucket_list.buckets[0].contacts) == 1,
                        "Expected 1 contact in bucket 0.")

        self.assertTrue(len(bucket_list.buckets[1].contacts) == 20,
                        "Expected 20 contacts in bucket 1.")

        self.assertTrue(new_contact not in bucket_list.buckets[1].contacts,
                        "Expected new contact NOT to replace an older contact.")


class DHTTest(unittest.TestCase):
    def test_local_store_find_value(self):
        vp = VirtualProtocol()
        # Below line should contain VirtualStorage(), which I don't have?
        dht = DHT(id=ID.random_id(),
                  protocol=vp,
                  storage_factory=VirtualStorage,
                  router=Router())
        # print(dht._originator_storage)
        vp.node = dht._router.node
        key = ID.random_id()
        dht.store(key, "Test")
        found, contacts, return_val = dht.find_value(key)
        print(found, contacts, return_val)
        self.assertTrue(return_val == "Test",
                        "Expected to get back what we stored.")

    def test_value_stored_in_closer_node(self):
        """
        This test creates a single contact and stores the value in that contact. We set up the IDs so that the
        contact’s ID is less (XOR metric) than our peer’s ID, and we use a key of ID.Zero to prevent further
        complexities when computing the distance. Most of the code here is to set up the conditions to make this test!
        - "The Kademlia Protocol Succinctly" by Marc Clifton
        :return: None
        """

        vp1 = VirtualProtocol()
        vp2 = VirtualProtocol()
        store1 = VirtualStorage()
        store2 = VirtualStorage()

        # Ensures that all nodes are closer, because id.max ^ n < id.max when n > 0.
        # (the distance between a node and max id is always closer than the furthest possible)
        dht = DHT(id=ID.max(), router=Router(), protocol=vp1, originator_storage=store1,
                  republish_storage=store1, cache_storage=VirtualStorage())

        vp1.node = dht._router.node
        contact_id: ID = ID.mid()  # middle ID
        other_contact: Contact = Contact(id=contact_id, protocol=vp2)
        other_node = Node(contact=other_contact, storage=store2)
        vp2.node = other_node

        # add this other contact to our peer list
        dht._router.node.bucket_list.add_contact(other_contact)
        # we want an integer distance, not an XOR distance.
        key: ID = ID.min()
        val = "Test"
        other_node.simply_store(key, val)
        self.assertFalse(store1.contains(key),
                         "Expected our peer to NOT have cached the key-value.")

        self.assertTrue(store2.contains(key),
                        "Expected other node to HAVE cached the key-value.")

        # Try and find the value, given our Dht knows about the other contact.
        _, _, retval = dht.find_value(key)
        self.assertTrue(retval == val,
                        "Expected to get back what we stored")

    def test_value_stored_in_further_node(self):
        vp1 = VirtualProtocol()
        vp2 = VirtualProtocol()
        store1 = VirtualStorage()
        store2 = VirtualStorage()

        # Ensures that all nodes are closer, because max id ^ n < max id when n > 0.

        dht: DHT = DHT(id=ID.min(), protocol=vp1, router=Router(), storage_factory=lambda: store1)

        vp1.node = dht._router.node
        contact_id = ID.max()
        other_contact = Contact(contact_id, vp2)
        other_node = Node(other_contact, store2)
        vp2.node = other_node
        # Add this other contact to our peer list.
        dht._router.node.bucket_list.add_contact(other_contact)
        key = ID(1)
        val = "Test"
        other_node.simply_store(key, val)

        self.assertFalse(store1.contains(key),
                         "Expected our peer to have NOT cached the key-value.")

        self.assertTrue(store2.contains(key),
                        "Expected other node to HAVE cached the key-value.")

        _, _, retval = dht.find_value(key)
        self.assertTrue(retval == val,
                        "Expected to get back what we stored.")

    def test_value_stored_gets_propagated(self):
        vp1 = VirtualProtocol()
        vp2 = VirtualProtocol()
        store1 = VirtualStorage()
        store2 = VirtualStorage()

        dht: DHT = DHT(id=ID.min(), protocol=vp1, router=Router(), storage_factory=lambda: store1)
        vp1.node = dht._router.node
        contact_id = ID.mid()
        other_contact = Contact(contact_id, vp2)
        other_node = Node(other_contact, store2)
        vp2.node = other_node
        dht._router.node.bucket_list.add_contact(other_contact)

        key = ID(0)
        val = "Test"

        self.assertFalse(store1.contains(key),
                         "Obviously we don't have the key-value yet.")

        self.assertFalse(store2.contains(key),
                         "And equally obvious, the other peer doesn't have the key-value yet either.")

        dht.store(key, val)

        self.assertTrue(store1.contains(key),
                        "Expected our peer to have stored the key-value.")

        self.assertTrue(store2.contains(key),
                        "Expected the other peer to have stored the key-value.")

    def test_value_propagates_to_closer_node(self):
        vp1 = VirtualProtocol()
        vp2 = VirtualProtocol()
        vp3 = VirtualProtocol()

        store1 = VirtualStorage()
        store2 = VirtualStorage()
        store3 = VirtualStorage()

        cache3 = VirtualStorage()

        dht: DHT = DHT(id=ID.max(),
                       protocol=vp1,
                       router=Router(),
                       originator_storage=store1,
                       republish_storage=store1,
                       cache_storage=VirtualStorage())
        vp1.node = dht._router.node

        # setup node 2
        contact_id_2 = ID.mid()
        other_contact_2 = Contact(contact_id_2, vp2)
        other_node_2 = Node(other_contact_2, store2)
        vp2.node = other_node_2
        # add the second contact to our peer list.
        dht._router.node.bucket_list.add_contact(other_contact_2)
        # node 2 has the value
        key = ID(0)
        val = "Test"
        other_node_2.storage.set(key, val)

        # setup node 3
        contact_id_3 = ID(2 ** 158)  # I think this is the same as ID.Zero.SetBit(158)?
        other_contact_3 = Contact(contact_id_3, vp3)
        other_node_3 = Node(other_contact_3, storage=store3, cache_storage=cache3)
        vp3.node = other_node_3
        # add the third contact to our peer list
        dht._router.node.bucket_list.add_contact(other_contact_3)

        self.assertFalse(store1.contains(key),
                         "Obviously we don't have the key-value yet.")

        self.assertFalse(store3.contains(key),
                         "And equally obvious, the third peer doesn't have the key-value yet either.")

        ret_found, ret_contacts, ret_val = dht.find_value(key)

        self.assertTrue(ret_found, "Expected value to be found.")
        self.assertFalse(store3.contains(key), "Key should not be in the republish store.")
        self.assertTrue(cache3.contains(key), "Key should be in the cache store.")
        self.assertTrue(cache3.get_expiration_time_sec(key.value) == Constants.EXPIRATION_TIME_SEC / 2,
                        "Expected 12 hour expiration.")


class DHTParallelTest(unittest.TestCase):
    """
    The exact same as DHTTest, but with the asynchronous router instead of the normal router.
    """

    def test_local_store_find_value(self):
        vp = VirtualProtocol()
        # Below line should contain VirtualStorage(), which I don't have?
        dht = DHT(id=ID.random_id(),
                  protocol=vp,
                  storage_factory=VirtualStorage,
                  router=ParallelRouter())
        vp.node = dht._router.node
        key = ID.random_id()
        dht.store(key, "Test")
        _, _, return_val = dht.find_value(key)

        self.assertTrue(return_val == "Test",
                        "Expected to get back what we stored.")

    def test_value_stored_in_closer_node(self):
        """
        This test creates a single contact and stores the value in that contact. We set up the IDs so that the
        contact’s ID is less (XOR metric) than our peer’s ID, and we use a key of ID.Zero to prevent further
        complexities when computing the distance. Most of the code here is to set up the conditions to make this test!
        - "The Kademlia Protocol Succinctly" by Marc Clifton
        :return: None
        """

        vp1 = VirtualProtocol()
        vp2 = VirtualProtocol()
        store1 = VirtualStorage()
        store2 = VirtualStorage()

        # Ensures that all nodes are closer, because id.max ^ n < id.max when n > 0.
        # (the distance between a node and max id is always closer than the furthest possible)
        dht = DHT(id=ID.max(), router=ParallelRouter(), storage_factory=lambda: store1, protocol=VirtualProtocol())

        vp1.node = dht._router.node
        contact_id: ID = ID.mid()  # middle ID
        other_contact: Contact = Contact(id=contact_id, protocol=vp2)
        other_node = Node(contact=other_contact, storage=store2)
        vp2.node = other_node

        # add this other contact to our peer list
        dht._router.node.bucket_list.add_contact(other_contact)
        # we want an integer distance, not an XOR distance.
        key: ID = ID.min()
        val = "Test"
        other_node.simply_store(key, val)
        self.assertFalse(store1.contains(key),
                         "Expected our peer to NOT have cached the key-value.")

        self.assertTrue(store2.contains(key),
                        "Expected other node to HAVE cached the key-value.")

        # Try and find the value, given our Dht knows about the other contact.
        _, _, retval = dht.find_value(key)
        self.assertTrue(retval == val,
                        "Expected to get back what we stored")

    def test_value_stored_in_further_node(self):
        vp1 = VirtualProtocol()
        vp2 = VirtualProtocol()
        store1 = VirtualStorage()
        store2 = VirtualStorage()

        # Ensures that all nodes are closer, because max id ^ n < max id when n > 0.

        dht: DHT = DHT(id=ID.min(), protocol=vp1, router=ParallelRouter(), storage_factory=lambda: store1)

        vp1.node = dht._router.node
        contact_id = ID.max()
        other_contact = Contact(contact_id, vp2)
        other_node = Node(other_contact, store2)
        vp2.node = other_node
        # Add this other contact to our peer list.
        dht._router.node.bucket_list.add_contact(other_contact)
        key = ID(1)
        val = "Test"
        other_node.simply_store(key, val)

        self.assertFalse(store1.contains(key),
                         "Expected our peer to have NOT cached the key-value.")

        self.assertTrue(store2.contains(key),
                        "Expected other node to HAVE cached the key-value.")

        retval: str = dht.find_value(key)[2]
        self.assertTrue(retval == val,
                        "Expected to get back what we stored.")

    def test_value_stored_gets_propagated(self):
        vp1 = VirtualProtocol()
        vp2 = VirtualProtocol()
        store1 = VirtualStorage()
        store2 = VirtualStorage()

        dht: DHT = DHT(id=ID.min(), protocol=vp1, router=ParallelRouter(), storage_factory=lambda: store1)
        vp1.node = dht._router.node
        contact_id = ID.mid()
        other_contact = Contact(contact_id, vp2)
        other_node = Node(other_contact, store2)
        vp2.node = other_node
        dht._router.node.bucket_list.add_contact(other_contact)

        key = ID(0)
        val = "Test"

        self.assertFalse(store1.contains(key),
                         "Obviously we don't have the key-value yet.")

        self.assertFalse(store2.contains(key),
                         "And equally obvious, the other peer doesn't have the key-value yet either.")

        dht.store(key, val)

        self.assertTrue(store1.contains(key),
                        "Expected our peer to have stored the key-value.")

        self.assertTrue(store2.contains(key),
                        "Expected the other peer to have stored the key-value.")

    # def test_value_propagates_to_closer_node(self):
    #     vp1 = VirtualProtocol()
    #     vp2 = VirtualProtocol()
    #     vp3 = VirtualProtocol()
    #
    #     store1 = VirtualStorage()
    #     store2 = VirtualStorage()
    #     store3 = VirtualStorage()
    #
    #     cache3 = VirtualStorage()
    #
    #     dht: DHT = DHT(id=ID.max(), protocol=vp1, router=ParallelRouter(), storage_factory=lambda: store1)
    #
    #     vp1.node = dht._router.node
    #
    #     # setup node 2
    #     contact_id_2 = ID.mid()
    #     other_contact_2 = Contact(contact_id_2, vp2)
    #     other_node_2 = Node(other_contact_2, store2)
    #     vp2.node = other_node_2
    #     # add the second contact to our peer list.
    #     dht._router.node.bucket_list.add_contact(other_contact_2)
    #     # node 2 has the value
    #     key = ID(0)
    #     val = "Test"
    #     other_node_2.storage.set(key, val)
    #
    #     # setup node 3
    #     contact_id_3 = ID(2 ** 158)  # I think this is the same as ID.Zero.SetBit(158)?
    #     other_contact_3 = Contact(contact_id_3, vp3)
    #     other_node_3 = Node(other_contact_3, store3, cache_storage=cache3)
    #     vp3.node = other_node_3
    #     # add the third contact to our peer list
    #     dht._router.node.bucket_list.add_contact(other_contact_3)
    #
    #     self.assertFalse(store1.contains(key),
    #                      "Obviously we don't have the key-value yet.")
    #
    #     self.assertFalse(store3.contains(key),
    #                      "And equally obvious, the third peer doesn't have the key-value yet either.")
    #
    #     ret_found, ret_contacts, ret_val = dht.find_value(key)
    #
    #     self.assertTrue(ret_found, "Expected value to be found.")
    #     self.assertFalse(store3.contains(key), "Key should not be in the republish store.")
    #     self.assertTrue(cache3.contains(key), "Key should be in the cache store.")
    #     self.assertTrue(cache3.get_expiration_time_sec(key.value) == Constants.EXPIRATION_TIME_SEC / 2,
    #                     "Expected 12 hour expiration.")


class BootstrappingTests(unittest.TestCase):

    def test_random_within_bucket_tests(self):

        test_cases: list[tuple[int, int]] = [

            (0, 256),  # 7 bits should be set
            (256, 1024),  # 2 bits (256 + 512) should be set
            (65536, 65536 * 2),  # no additional bits should be set.
            (65536, 65536 * 4),  # 2 bits (65536 and 65536*2) should be set.
            (65536, 65536 * 16)  # 4 bits (65536, 65536*2, 65536*4, 65536*8) set.
        ]
        for test_case in test_cases:
            low = test_case[0]
            high = test_case[1]
            bucket: KBucket = KBucket(low=low, high=high)

            id: ID = ID.random_id_within_bucket_range(bucket)
            self.assertTrue(bucket.is_in_range(id))

    def test_bootstrap_within_bootstrapping_bucket(self):
        """
        Test the bootstrap process within a bootstrapping bucket scenario.

        This test creates a network with 22 virtual protocols representing nodes.
        It sets up a bootstrap peer, with the bootstrapper having knowledge of 10 other nodes,
        and one of those nodes having knowledge of another 10 nodes. The goal is to simulate
        the bootstrap process and ensure that the expected number of contacts is received.

        We need 22 virtual protocols. One for the bootstrap peer,
        10 for the nodes the bootstrap peer knows about, and 10 for
        the nodes one of the nodes knows about. And one for us to
        rule them all.
        :return: None
        """
        vp: list[VirtualProtocol] = []
        # creates 22 virtual protocols
        for i in range(22):
            vp.append(VirtualProtocol())

        # us
        dht_us: DHT = DHT(ID.random_id(), vp[0], storage_factory=VirtualStorage, router=Router())
        vp[0].node = dht_us._router.node

        # our bootstrap peer
        dht_bootstrap: DHT = DHT(ID.random_id(), vp[1], storage_factory=VirtualStorage, router=Router())
        vp[1].node = dht_bootstrap._router.node

        # stops pycharm saying it could be undefined later on. THIS LINE IS USELESS.
        n: Node = Node(Contact(ID.random_id(), vp[0]), VirtualStorage())

        # Our bootstrapper knows 10 contacts
        for i in range(10):
            c: Contact = Contact(ID.random_id(), vp[i + 2])
            n: Node = Node(c, VirtualStorage())
            vp[i + 2].node = n
            dht_bootstrap._router.node.bucket_list.add_contact(c)

        # One of those nodes, in this case the last one we added to our bootstrapper (for convenience's sake)
        # knows about 10 other contacts

        # n is the last one our bootstrapper knows
        node_who_knows_10 = n  # Ignore PyCharm error saying it can be referenced before being created.
        del n  # bad naming, don't want to use it later on.

        # create the 10 it knows about
        for i in range(10):
            c: Contact = Contact(ID.random_id(), vp[i + 12])
            n2: Node = Node(c, VirtualStorage())
            vp[i + 12].node = n2
            node_who_knows_10.bucket_list.add_contact(c)

        dht_us.bootstrap(dht_bootstrap._router.node.our_contact)

        sum_of_contacts = len(dht_us._router.node.bucket_list.contacts())
        print(f"sum of contacts: {sum_of_contacts}")
        self.assertTrue(sum_of_contacts == 11,
                        "Expected our peer to get 11 contacts.")

    def test_bootstrap_outside_bootstrapping_bucket(self):
        """
        Test the bootstrap process when bootstrapping from a DHT node with contacts outside its own bucket.

        This test simulates the scenario where a DHT node (dht_us) attempts to bootstrap from another node
        (dht_bootstrap) whose contact list includes nodes outside its immediate bucket range. The goal is to
        verify that the bootstrapping process correctly adds and organizes contacts, and handles bucket splits.

        Steps:
        1. Create two DHT nodes, dht_us and dht_bootstrap, and establish virtual protocols (vp) for communication.
        2. Populate dht_bootstrap with 20 contacts, with one contact having an ID >= 2 ** 159 to trigger a bucket split.
        3. Add 10 contacts to one of the nodes in dht_bootstrap's contact list (simulating contacts outside the bucket).
        4. Perform bootstrap operation from dht_us to dht_bootstrap.
        5. Verify that dht_us has a total of 31 contacts after bootstrapping.

        Raises:
            AssertionError: If the number of contacts in dht_us after bootstrapping is not 31.
            """
        vp: list[VirtualProtocol] = []
        for i in range(32):
            vp.append(VirtualProtocol())

        # Us, ID doesn't matter.
        dht_us: DHT = DHT(ID.random_id(), vp[0], storage_factory=VirtualStorage, router=Router())
        vp[0].node = dht_us._router.node

        # our bootstrap peer
        # all IDs are < 2 ** 159
        dht_bootstrap: DHT = DHT(ID.random_id(0, 2 ** 159 - 1), vp[1], storage_factory=VirtualStorage, router=Router())
        vp[1].node = dht_bootstrap._router.node
        # print(sum([len([c for c in b.contacts]) for b in dht_bootstrap._router.node.bucket_list.buckets]))

        # Our bootstrapper knows 20 contacts
        for i in range(19):
            # creating 19 shell contacts
            id: ID = ID.random_id(0, 2 ** 159 - 1)
            c: Contact = Contact(id, vp[i + 2])
            c.protocol.node = Node(c, VirtualStorage())
            dht_bootstrap._router.node.bucket_list.add_contact(c)

        # for 20th
        # all IDs are < 2 ** 159, except the last one, which is >= 2 ** 159
        # Which will force a bucket split for us
        id = ID.max()
        important_contact: Contact = Contact(id, vp[21])
        n = Node(important_contact, VirtualStorage())

        # add 10 contacts to node
        # this basically means that the bootstrapper knows 20 contacts, one of them knows 10 contacts.
        # we're trying to add all 30 + bootstrapper so 31.
        for i in range(10):
            # creating 10 shell contacts
            c2: Contact = Contact(ID.random_id(), vp[i + 22])
            n2 = Node(c2, VirtualStorage())
            vp[i + 22].node = n2
            # adding the 10 shell contacts
            n.bucket_list.add_contact(c2)  # Note we're adding these contacts to the 10th node.

        important_contact.protocol.node = n
        dht_bootstrap._router.node.bucket_list.add_contact(important_contact)  # adds the 1 important contact.

        # just making sure vp[i + 2].node = n works retrospectively on c.

        self.assertTrue(n.our_contact.id == important_contact.protocol.node.our_contact.id == ID.max())

        self.assertTrue(len(n.bucket_list.contacts()) == 10,
                        f"contacts: {len(n.bucket_list.contacts())}")

        self.assertTrue(len(important_contact.protocol.node.bucket_list.contacts()) == 10,
                        f"contacts: {len(n.bucket_list.contacts())}")

        self.assertTrue(important_contact.id == ID.max(), "What else could it be?")

        self.assertTrue(ID.max() in [c.id for c in dht_bootstrap._router.node.bucket_list.contacts()],
                        "Contact we just added to bucket list should be in bucket list.")

        # print("DHT Bootstrap contact length =", len(dht_bootstrap._router.node.bucket_list.contacts()))
        self.assertTrue(len(dht_bootstrap._router.node.bucket_list.contacts()) == 20,
                        "DHT Bootstrapper must have 20 contacts.")
        # One of those nodes, in this case specifically the last one we added to our bootstrapper so that it isn't in
        # the bucket of our bootstrapper, we add 10 contacts. The IDs of those contacts don't matter.

        self.assertTrue([len(b.contacts) for b in n.bucket_list.buckets] == [10],
                        "Must have 10 contacts in node.")

        # print("Starting bootstrap...")
        dht_us.bootstrap(dht_bootstrap._router.node.our_contact)
        # print("Bootstrap finished!")

        # print(f"\nLength of buckets: {[len(b.contacts) for b in dht_us._router.node.bucket_list.buckets]}")

        sum_of_contacts = len(dht_us._router.node.bucket_list.contacts())
        self.assertTrue(sum_of_contacts == 31,
                        f"Expected our peer to have 31 contacts, {sum_of_contacts} were given.")


class BucketManagementTests(unittest.TestCase):
    def test_non_responding_contact_evicted(self):
        """
        Tests that a nonresponding contact is evicted after 
        Constants.EVICTION_LIMIT tries.
        """
        dht = DHT(ID(0), VirtualProtocol(), storage_factory=VirtualStorage, router=Router())
        bucket_list: BucketList = setup_split_failure(dht.node.bucket_list)
        self.assertTrue(len(bucket_list.buckets) == 2,
                        "Bucket split should have occurred.")
        self.assertTrue(len(bucket_list.buckets[0].contacts) == 1,
                        "Expected 1 contact in bucket 0.")
        self.assertTrue(len(bucket_list.buckets[1].contacts) == 20,
                        "Expected 20 contacts in bucket 1.")

        # The bucket is now full. Pick the first contact, as it is last 
        # seen (they are added in chronological order.)

        non_responding_contact: Contact = bucket_list.buckets[1].contacts[0]
        # Since the protocols are shared, we need to assign 
        # a unique protocol for this node for testing.
        vp_unresponding: VirtualProtocol = VirtualProtocol(
            non_responding_contact.protocol.node,
            False
        )
        non_responding_contact.protocol = vp_unresponding

        next_new_contact = Contact(
            ID(2 ** 159 + 1),
            dht.our_contact.protocol
        )

        # Hit the nonresponding contact EVICTION_LIMIT times
        # Which will trigger the eviction algorithm.

        for _ in range(Constants.EVICTION_LIMIT):
            bucket_list.add_contact(next_new_contact)

        self.assertTrue(len(bucket_list.buckets[1].contacts) == 20,
                        "Expected 20 contacts in bucket 1.")

        self.assertTrue(len(bucket_list.buckets[0].contacts) == 1,
                        f"Expected 1 contact in bucket 0, got {len(bucket_list.buckets[0].contacts)}.")

        # Verify can_split -> pending eviction happened
        self.assertTrue(len(dht.pending_contacts) == 0,
                        "Pending contact list should now be empty.")
        self.assertFalse(non_responding_contact in bucket_list.contacts(),
                         "Expected bucket to NOT contain non-responding contact.")

        self.assertTrue(next_new_contact in bucket_list.contacts(),
                        "Expected bucket to contain new contact.")

        self.assertTrue(len(dht.eviction_count) == 0,
                        "Expected no contacts to be pending eviction.")

    def test_non_responding_contact_delayed_eviction(self):
        """
        Tests that a nonresponding contact puts the new contact into a pending list.
        """
        dht = DHT(ID(0), VirtualProtocol(), storage_factory=VirtualStorage, router=Router())
        bucket_list: BucketList = setup_split_failure(dht.node.bucket_list)

        self.assertTrue(len(bucket_list.buckets) == 2,
                        "Bucket split should have occurred.")

        self.assertTrue(len(bucket_list.buckets[0].contacts) == 1,
                        "Expected 1 contact in bucket 0.")

        self.assertTrue(len(bucket_list.buckets[1].contacts) == 20,
                        "Expected 20 contacts in bucket 1.")

        # The bucket is now full. pick the first contact,
        # as it is last seen (they are added chronologically.)
        non_responding_contact: Contact = bucket_list.buckets[1].contacts[0]

        # Since the protocols are shared, we assign a unique protocol for this node for testing.
        vp_unresponding = VirtualProtocol(
            node=non_responding_contact.protocol.node,
            responds=False
        )
        non_responding_contact.protocol = vp_unresponding

        # set up the next new contact (it can respond.)
        next_new_contact = Contact(
            id=ID(2 ** 159 + 1),
            protocol=dht.our_contact.protocol
        )
        bucket_list.add_contact(next_new_contact)

        self.assertTrue(len(bucket_list.buckets[1].contacts) == 20,
                        f"Expecting 20 contacts in bucket 1, got {len(bucket_list.buckets[0].contacts)}")

        self.assertTrue(len(bucket_list.buckets[0].contacts) == 1,
                        f"Expected 1 contact in bucket 0, got {len(bucket_list.buckets[0].contacts)}")

        # Verify can_split -> Evict happened.

        self.assertTrue(len(dht.pending_contacts) == 1,
                        "Expected one pending contact.")
        self.assertTrue(next_new_contact in dht.pending_contacts,
                        "Expected pending contact to be the 21st contact.")
        self.assertTrue(len(dht.eviction_count) == 1,
                        "Expected one contact to be pending eviction.")


class Chapter10Tests(unittest.TestCase):
    def test_new_contact_gets_stored_contacts(self):
        """
        Verify that we get stored values whose keys ^ contact ID are less than stored keys ^ other contacts.

        There’s a lot of setup here for creating two existing contacts and two 
        key-values whose IDs have been specifically set. See the comments for 
        the XOR distance “math.”
        """

        # Set up a node at the midpoint
        # The existing node haas the ID 10000...
        existing: Node = Node(Contact(ID.mid(), None), VirtualStorage())
        val_1 = "Value 1"
        val_mid = "Value mid"

        # The existing node stores the 2 items, one with an ID "hash" of 1, the other with ID.max.
        # Simple storage rather than executing the code for store.
        existing.simply_store(ID(1), val_1)
        existing.simply_store(ID.mid(), val_mid)

        self.assertTrue(len(existing.storage.get_keys()) == 2,
                        f"Expected the existing node to have 2 key-values. {existing.storage.get_keys()}")

        # Create a contact in the existing node's bucket list that is closer to one of the values.
        # This contact has the prefix 0100000...
        other_contact = Contact(ID(2 ** 158), None)
        other = Node(other_contact, VirtualStorage())
        existing.bucket_list.buckets[0].contacts.append(other_contact)

        # The unseen contact has prefix 0110000...
        unseen_vp = VirtualProtocol()
        unseen_contact = Contact(ID(2 ** 158 + 2 ** 157), unseen_vp)
        unseen = Node(unseen_contact, VirtualStorage())
        unseen_vp.node = unseen  # final fixup

        self.assertTrue(len(unseen.storage.get_keys()) == 0, "The unseen node shouldn't have any key-values!")

        # An unseen node pings, and we should get back val_min only as ID(1) ^ ID.mid() < ID.max() ^ ID.mid()

        self.assertTrue(ID(1) ^ ID.mid() < ID.max() ^ ID.mid(), (f"Fundamental issue with ID class. "
                                                                 f"\n{ID(ID(1) ^ ID.mid()).bin()} \nvs "
                                                                 f"\n{ID(ID.max() ^ ID.mid()).bin()}"))
        existing.ping(unseen_contact)

        # Contacts   V1        V2
        # 1000000 00...0001 10...0000
        # 0100000
        # maths:
        # c1 ^ v1    c1 ^ v2    c2 ^ v1    c2 ^ v2
        # 100...001  000...000  010...001  110...000
        # c1 ^ v1 > c1 ^ v2, so v1 doesn't get send to the unseen node.
        # c1 ^ v2 < c2 ^ v2, so it does get sent.

        self.assertTrue(
            len(unseen.storage.get_keys()) == 1,
            "Expected 1 value stored in our new node."
        )

        self.assertTrue(
            unseen.storage.contains(ID.mid()),
            "Expected val_mid to be stored."
        )

        self.assertTrue(
            unseen.storage.get(ID.mid()) == val_mid,
            f"Expected val_mid value to match, got {unseen.storage.get(ID.mid())}"
        )


class DHTSerialisationTests(unittest.TestCase):
    def test_serialisation(self):
        dht: DHT = DHT(
            id=ID.random_id(),
            protocol=VirtualProtocol(),
            router=Router(),
            storage_factory=VirtualStorage
        )
        dht.save("kademlia_dht/dht.pickle")

        new_dht = DHT.load("kademlia_dht/dht.pickle")

        self.assertTrue(
            type(dht) == type(new_dht),
            f"Saved and loaded DHT are not the same type. {type(dht)} vs {type(new_dht)}"
        )
        self.assertTrue(
            dht.our_id == new_dht.our_id,
            "Saved and loaded DHT is not identical to the original."
        )

    def test_circular_serialisation(self):
        dht: DHT = DHT(
            id=ID.random_id(),
            protocol=VirtualProtocol(),
            router=Router(),
            storage_factory=VirtualStorage
        )

        node = Node(
            Contact(dht.our_id),
            storage=VirtualStorage()
        )
        dht._router.node = node

        dht.save("kademlia_dht/dht.pickle")

        new_dht = DHT.load("kademlia_dht/dht.pickle")

        self.assertTrue(
            type(dht) == type(new_dht),
            "Saved and loaded DHT are not the same type. "
            f"{type(dht)} vs {type(new_dht)}"
        )

        self.assertTrue(
            dht._router.node.our_contact.id == new_dht._router.node.our_contact.id,
            "Saved and loaded DHT is not identical to the original."
        )

    def second_dht_serialisation_test(self):
        p1: TCPSubnetProtocol = TCPSubnetProtocol(
            "http://127.0.0.1/",
            7124,
            1
        )

        p2: TCPSubnetProtocol = TCPSubnetProtocol(
            "http://127.0.0.1/",
            7124,
            1
        )

        store1: VirtualStorage = VirtualStorage()
        store2: VirtualStorage = VirtualStorage()

        # Ensures that all nodes are closer, becuase ID.max() ^ n < ID.max()
        # When n > 0
        dht: DHT = DHT(
            id=ID.max(),
            protocol=p1,
            router=Router(),
            originator_storage=store1,
            republish_storage=store1,
            cache_storage=VirtualStorage()
        )

        contact_id: ID = ID.mid()
        other_contact: Contact = Contact(
            id=contact_id,
            protocol=p2
        )
        other_node: Node = Node(
            contact=other_contact,
            storage=store2
        )
        # Add this other contact to our peer list.
        dht.node.bucket_list.add_contact(other_contact)
        dht.save(f"dht.{Constants.DHT_SERIALISED_SUFFIX}")

        new_dht: DHT = DHT.load(f"dht.{Constants.DHT_SERIALISED_SUFFIX}")
        self.assertTrue(
            new_dht.node.bucket_list.contacts() == 1,
            "Expected our node to have 1 contact."
        )
        self.assertTrue(
            new_dht.node.bucket_list.contact_exists(other_contact),
            "Expected our contact to have the other contact."
        )
        self.assertTrue(
            new_dht._router.node == new_dht.node,
            "Router node not initialised."
        )


class TCPSubnetTests(unittest.TestCase):
    @staticmethod
    def setup():
        local_ip = "127.0.0.1"
        valid_server = False
        server = None
        port = 1
        while not valid_server:
            port = random.randint(10000, 10500)
            server = TCPSubnetServer(server_address=(local_ip, port))
            valid_server = True

        p1: TCPSubnetProtocol = TCPSubnetProtocol(url=local_ip, port=port, subnet=1)
        p2: TCPSubnetProtocol = TCPSubnetProtocol(url=local_ip, port=port, subnet=2)

        our_id = ID.random_id()

        c1 = Contact(id=our_id, protocol=p1)
        c2 = Contact(id=ID.random_id(), protocol=p2)

        n1 = Node(c1, VirtualStorage())
        n2 = Node(c2, VirtualStorage())

        server.register_protocol(p1.subnet, n1)
        server.register_protocol(p2.subnet, n2)
        # print(server.subnets)
        thread = server.thread_start()

        return local_ip, port, server, p1, p2, our_id, c1, c2, n1, n2, thread

    def test_ping_route(self):
        """
        Makes sure no exceptions are thrown when pinging a contact.
        """
        local_ip, port, server, p1, p2, our_id, c1, c2, n1, n2, thread = self.setup()

        # The actual test:
        p2.ping(c1)

        server.thread_stop(thread)

    def test_store_route(self):
        local_ip, port, server, p1, p2, our_id, c1, c2, n1, n2, thread = self.setup()

        # The actual test:

        sender = Contact(ID.random_id(), p1)
        test_id: ID = ID.random_id()
        test_value = "Test"
        p2.store(sender, test_id, test_value)

        self.assertTrue(n2.storage.contains(test_id),
                        "Expected remote peer to have value.")
        self.assertTrue(n2.storage.get(test_id) == test_value,
                        "Expected remote peer to contain stored value.")

        server.thread_stop(thread)

    def test_find_nodes_route(self):
        print()
        local_ip = "127.0.0.1"
        valid_server = False
        port = None
        server = None
        while not valid_server:
            port = random.randint(10000, 10500)
            server = TCPServer(subnet_server_address=(local_ip, port))
            valid_server = True

        p1 = TCPSubnetProtocol(url=local_ip, port=port, subnet=1)
        p2 = TCPSubnetProtocol(url=local_ip, port=port, subnet=2)

        our_id = ID.random_id()

        c1 = Contact(id=our_id, protocol=p1)
        c2 = Contact(id=ID.random_id(), protocol=p2)

        n1 = Node(c1, VirtualStorage())
        n2 = Node(c2, VirtualStorage())

        # Node 2 knows about another contact that isn't us
        # - this is what we are trying to find

        other_peer = ID.random_id()

        n2.bucket_list.buckets[0].contacts.append(
            Contact(
                other_peer,
                TCPSubnetProtocol(local_ip, port, 3)
            )
        )
        server.register_protocol(p1.subnet, n1)
        server.register_protocol(p2.subnet, n2)
        thread = server.thread_start()

        id = ID.random_id()
        ret, errors = p2.find_node(c1, id)
        print()
        print("ret", ret)
        print("errors", errors)
        if ret:
            self.assertTrue(
                len(ret) == 1,
                f"Expected 1 contact, {len(ret)} were returned."
            )

            self.assertTrue(
                ret[0].id == other_peer,
                "Expected contact to the other peer (not us).")
        else:
            self.assertTrue(
                type(ret) == list[Contact],
                "Expected find_node to return 1 contact, 0 were returned."
            )

        server.thread_stop(thread)

    def test_find_value_router(self):
        local_ip, port, server, p1, p2, our_id, c1, c2, n1, n2, thread = self.setup()

        # Node 2 knows about another contact that isn't us
        # - this is what we are trying to find

        test_id = ID.random_id()
        test_value = "Test"
        print("[Unit test] Store starting...")
        p2.store(sender=c1, key=test_id, val=test_value)
        print("[Unit test] Store done.")
        self.assertTrue(
            n2.storage.contains(test_id),
            "Expected remote peer to have value."
        )

        self.assertTrue(
            n2.storage.get(test_id) == test_value,
            "Expected node to store the correct value."
        )

        print("[Unit test] Find value starting...")
        contacts, val, error = p2.find_value(c1, test_id)
        print("[Unit test] Find value received:", contacts, val, error)
        print("[Unit test] Find value done.")

        self.assertFalse(
            contacts, "Expected to find value."  # huh?
        )
        print(f"We stored '{val}' on the other node, we got back '{test_value}'.")
        self.assertTrue(
            val == test_value, "Value does not match expected value from peer."
        )

    def test_unresponsive_node(self):
        local_ip = "127.0.0.1"
        valid_server = False
        server = None
        port = 1
        while not valid_server:
            port = random.randint(10000, 10500)
            server = TCPServer(subnet_server_address=(local_ip, port))
            valid_server = True

        p1 = TCPSubnetProtocol(url=local_ip, port=port, subnet=1)
        p2 = TCPSubnetProtocol(url=local_ip, port=port, subnet=2)
        p2.responds = False

        our_id = ID.random_id()

        c1 = Contact(id=our_id, protocol=p1)
        c2 = Contact(id=ID.random_id(), protocol=p2)

        n1 = Node(c1, VirtualStorage())
        n2 = Node(c2, VirtualStorage())

        server.register_protocol(p1.subnet, n1)
        server.register_protocol(p2.subnet, n2)
        thread = server.thread_start()

        test_id = ID.random_id()
        test_value = "Test"

        error: RPCError = p2.store(c1, test_id, test_value)
        # print("[Unit tests] [Error]", error)
        self.assertTrue(
            error.timeout_error,
            "Expected timeout when contacting unresponsive node."
        )

        server.thread_stop(thread)


class JSONStorageTests(unittest.TestCase):
    def test_get_set(self):
        if os.path.exists("1"):
            shutil.rmtree("1")
        storage = SecondaryJSONStorage(f"{ID(1)}/test_storage.json")
        store_id = ID(1)
        storage.set(store_id, "Test")
        self.assertTrue(storage.contains(store_id), "Expected storage to contain data")
        ret_val = storage.get(store_id)
        self.assertEqual(ret_val, "Test")

    def test_remove(self):
        if os.path.exists("1"):
            shutil.rmtree("1")
        storage = SecondaryJSONStorage(f"{ID(1)}/test_storage.json")
        storage.set(ID(2), "to remove")
        self.assertTrue(storage.contains(2), "We should have added the ID.")
        storage.remove(2)
        self.assertFalse(storage.contains(2), "Should have removed the ID.")


class IDIntegerTests(unittest.TestCase):
    def test_xor(self):
        id_23 = ID(23)
        self.assertTrue(ID(23) ^ 14 == 23 ^ 14)  # Typical
        self.assertTrue(ID(14) ^ 23 == 14 ^ 23)  # Typical
        self.assertTrue(ID(2352) ^ 53 == 2352 ^ 53)  # Typical
        self.assertTrue(ID(0) ^ 0 == 0 ^ 0)  # Boundary
        self.assertTrue(ID(2 ** 160 - 1) ^ 4 == (2 ** 160 - 1) ^ 4)  # Boundary

    def test_ranges(self):
        with self.assertRaises(ValueError):
            overrange_id = ID(2 ** 160)  # Boundary Erroneous

        with self.assertRaises(ValueError):
            overrange_id = ID(2 ** 160 + 7)  # Erroneous

        with self.assertRaises(ValueError):
            overrange_id = ID(-1)  # Erroneous

    def test_equal(self):
        self.assertTrue(ID(1) == 1)
        self.assertTrue(ID(34) == 34)

    def test_lt(self):
        self.assertTrue(ID(1) < 2)
        self.assertTrue(ID(54) < 70)

    def test_le(self):
        self.assertTrue(ID(1) <= 2)
        self.assertTrue(ID(2) <= 2)
        self.assertTrue(ID(54) <= 70)
        self.assertTrue(ID(70) <= 70)

    def test_gt(self):
        self.assertTrue(ID(1) >= 1)
        self.assertTrue(ID(1) >= 0)
        self.assertTrue(ID(100) >= 100)
        self.assertTrue(ID(2 ** 160 - 1) >= 1)


class NodeLookupTests(unittest.TestCase):
    def test_get_close_contacts_ordered(self):
        """
        Description

        Adds 100 random contacts to a nodes bucket list, then FIND_NODE is performed.

        Expected

        K Contacts should be returned; Returned contacts should be ordered by distance.
        It should have returned the smallest ID’s possible, as host ID = 0.

        :return:
        """
        sender: Contact = Contact(id=ID.random_id(),
                                  protocol=None)
        node: Node = Node(
            Contact(id=ID.random_id(), protocol=None),
            VirtualStorage())

        contacts: list[Contact] = []
        for _ in range(100):
            contacts.append(
                Contact(id=ID.random_id(), protocol=None))

        for contact in contacts:
            node.bucket_list.add_contact(contact)

        key: ID = ID.random_id()

        closest: list[Contact] = node.find_node(sender=sender, key=key)[0]
        self.assertTrue(len(closest) == Constants.K,
                        "Expected K contacts to be returned.")

        # the contacts are already in ascending order with respect to the key.
        distances: list[int] = [c.id ^ key for c in closest]
        distance: int = distances[0]

        # checking they're all in order (ascending)
        for i in distances[1:]:
            self.assertTrue(distance < i,
                            "Expected contacts to be ordered by distance.")
            distance = i

        # Verify the contacts with the smallest distances have been returned from all possible distances.
        largest_close_contact = distances[-1]

        # This just makes sure it returned the K smallest contact ID's possible.
        others = []
        for b in node.bucket_list.buckets:
            for c in b.contacts:
                if c not in closest and (c.id ^ key) < largest_close_contact and c.id != sender.id:
                    others.append(c)

        self.assertTrue(
            len(others) == 0,
            "Expected no other contacts with a smaller distance than the greatest distance to exist, "
            f"found {len(others)} {f"with distance {others[0].id ^ key}" if len(others) == 1 else ''}."
        )

    def test_no_nodes_to_query(self):
        """
        Creates K nodes and adds them to our routers bucket list, where each node knows about
        the other peers.
        :return:
        """
        router_node_contact = Contact(id=ID.random_id(),
                                      protocol=None)
        router = Router(
            node=Node(contact=router_node_contact, storage=VirtualStorage()))

        nodes: list[Node] = []

        for i in range(Constants.K):
            nodes.append(
                Node(Contact(id=ID(2 ** i)), storage=VirtualStorage()))

        for n in nodes:
            # fixup protocols
            n.our_contact.protocol = VirtualProtocol(n)

            # our contacts:
            router.node.bucket_list.add_contact(n.our_contact)

            # each peer needs to know about the other peers
            n_other = [i for i in nodes if i is not n]  # MIGHT ERROR
            # n_other = [i for i in nodes if i != n]

            # From book:
            # nodes.ForEach(n => nodes.Where(nOther => nOther != n).
            # ForEach(nOther => n.BucketList.AddContact(nOther.OurContact)));
            for other_node in n_other:
                n.bucket_list.add_contact(other_node.our_contact)

        # select the key such that n^0==n
        key = ID(0)
        # all contacts are in one bucket (?)
        contacts_to_query = router.node.bucket_list.buckets[0].contacts
        closer_contacts: list[Contact] = []
        further_contacts: list[Contact] = []

        for c in contacts_to_query:
            # should I read the output?
            found, val, found_by, closer_contacts, further_contacts = \
                router.get_closer_nodes(key=key,
                                        node_to_query=c,
                                        rpc_call=router.rpc_find_nodes,
                                        closer_contacts=closer_contacts,
                                        further_contacts=further_contacts)

            closer_compare_arr = []
            for contact in further_contacts:
                if contact.id not in [i.id for i in contacts_to_query]:
                    closer_compare_arr.append(contact)

            self.assertTrue(len(closer_compare_arr) == 0, "No new nodes expected.")

            further_compare_arr = []
            for contact in further_contacts:
                if contact.id not in [i.id for i in contacts_to_query]:
                    further_compare_arr.append(contact)

            self.assertTrue(len(further_compare_arr) == 0, "No new nodes expected.")

    def __setup(self):
        self.router = Router(
            Node(Contact(id=ID.random_id(), protocol=None),
                 storage=VirtualStorage()))

        self.nodes: list[Node] = []
        for _ in range(100):
            contact: Contact = Contact(id=ID.random_id(), protocol=VirtualProtocol())
            node: Node = Node(contact, VirtualStorage())
            contact.protocol.node = node
            self.nodes.append(node)

        for n in self.nodes:
            n.our_contact.protocol = VirtualProtocol(n)  # Fix up protocols
            self.router.node.bucket_list.add_contact(n.our_contact)
            for other_n in self.nodes:  # let each node know about each other node
                if other_n != n:
                    n.bucket_list.add_contact(other_n.our_contact)

        # pick a random bucket
        key = ID.random_id()
        # take "A" contacts from a random KBucket
        self.contacts_to_query: list[Contact] = \
            self.router.node.bucket_list.get_kbucket(key).contacts[:Constants.A]

        self.closer_contacts: list[Contact] = []
        self.further_contacts: list[Contact] = []

        self.closer_contacts_alt_computation: list[Contact] = []
        self.further_contacts_alt_computation: list[Contact] = []

        self.nearest_contact_node = sorted(self.contacts_to_query,
                                           key=lambda contacts_to_query_nodes: contacts_to_query_nodes.id ^ key)[0]
        self.distance = self.nearest_contact_node.id ^ key

    def get_alt_close_and_far(self, contacts_to_query: list[Contact],
                              closer: list[Contact],
                              further: list[Contact],
                              nodes: list[Node],
                              key: ID,  # I think this is needed.
                              distance
                              ):
        """
        Alternate implementation for getting closer and further contacts.
        """
        # For each node (A == K) for testing in our bucket (nodes_to_query
        for contact in contacts_to_query:
            # Find the node that we're contacting:
            contact_node: Node = next((n for n in nodes if n.our_contact == contact), None)
            if contact_node is None:
                continue

            # Close contacts except ourself and the nodes we're contacting.
            # Note that of all the contacts in the bucket list, many of the K returned
            # by the get_close_contacts call are contacts we're querying, so they're being excluded.
            close_contacts_of_contacted_node = [
                c for c in contact_node.bucket_list.get_close_contacts(key, self.router.node.our_contact.id)
                if c.id.value not in [c.id.value for c in contacts_to_query]
            ]

            for close_contact_of_contacted_node in close_contacts_of_contacted_node:
                # Which of these contacts are closer?
                if (
                        close_contact_of_contacted_node.id.value ^ key.value < distance and close_contact_of_contacted_node.id.value not in
                        [c.id.value for c in closer]):
                    closer.append(close_contact_of_contacted_node)

                # Which of these contacts are farther?
                if close_contact_of_contacted_node.id.value ^ key.value >= distance and close_contact_of_contacted_node.id.value not in [
                    c.id.value for c in further]:
                    further.append(close_contact_of_contacted_node)

    def test_simple_all_closer_contacts(self):
        # setup
        # by selecting our node ID to zero, we ensure that all distances of other nodes
        # are greater than the distance to our node.

        # Create a router with the largest ID possible.
        router = Router(Node(Contact(id=ID.max(), protocol=None), VirtualStorage()))
        nodes: list[Node] = []

        for n in range(Constants.K):
            # Create a node with id of a power of 2, up to 2**20.
            node = Node(Contact(id=ID(2 ** n), protocol=None), storage=VirtualStorage())
            nodes.append(node)

        # Fixup protocols
        for n in nodes:
            n.our_contact.protocol = VirtualProtocol(n)

        # add all contacts in our node list to the router.
        for n in nodes:
            router.node.bucket_list.add_contact(n.our_contact)

        # let all of them know where the others are:
        # (add each nodes contact to each nodes bucket_list)
        for n in nodes:
            for n_other in nodes:
                if n != n_other:
                    n.bucket_list.add_contact(n_other.our_contact)

        # select the key such that n ^ 0 == n (TODO: Why?)
        # this ensures the distance metric uses only the node ID,
        # which makes for an integer difference for distance, not an XOR distance.
        key = ID(0)
        # all contacts are in one bucket
        # This is because we added K node's contacts to router,
        # so it shouldn't have split.
        # contacts_to_query = router.node.bucket_list.buckets[0].contacts

        find_result = router.lookup(key=key,
                                    rpc_call=router.rpc_find_nodes,
                                    give_me_all=True)

        contacts = find_result["contacts"]

        # Make sure lookup returns K contacts.
        self.assertTrue(len(contacts) == Constants.K, f"Expected K closer contacts, got {len(contacts)}. {contacts}")

        # Make sure it realises all contacts should be closer than 2**160 - 1.
        self.assertTrue(len(router.closer_contacts) == Constants.K,
                        "All contacts should be closer than the ID 2**160 - 1.")

        self.assertTrue(len(router.further_contacts) == 0,
                        f"Expected no further contacts, got {[c.id for c in router.further_contacts]}")

    def test_simple_all_further_contacts(self):
        # setup
        # by selecting our node ID to zero, we ensure that all distances of other nodes
        # are greater than the distance to our node.

        # Create a router with the smallest ID possible.
        # By selecting our node ID to zero, we ensure that all distances of other nodes are > the distance to our node.
        router = Router(Node(Contact(id=ID(0), protocol=None), VirtualStorage()))
        nodes: list[Node] = []

        for n in range(Constants.K):
            # Create a node with id of a power of 2, up to 2**20.
            node = Node(Contact(id=ID(2 ** n), protocol=None), storage=VirtualStorage())
            nodes.append(node)

        # Fixup protocols
        for n in nodes:
            n.our_contact.protocol = VirtualProtocol(n)

        # add all contacts in our node list to the router.
        for n in nodes:
            router.node.bucket_list.add_contact(n.our_contact)

        # let all of them know where the others are:
        # (add each nodes contact to each nodes bucket_list)
        for n in nodes:
            for n_other in nodes:
                if n != n_other:
                    n.bucket_list.add_contact(n_other.our_contact)

        # select the key such that n ^ 0 == n
        # this ensures the distance metric uses only the node ID,
        # which makes for an integer difference for distance, not an XOR distance.
        key = ID(0)
        # all contacts are in one bucket
        # This is because we added K node's contacts to router,
        # so it shouldn't have split.

        find_result = router.lookup(key=key,
                                    rpc_call=router.rpc_find_nodes,
                                    give_me_all=True)

        contacts = find_result["contacts"]

        # Make sure lookup returns K contacts.
        self.assertTrue(len(contacts) == 0, f"Expected 0 closer contacts, got {len(contacts)}.")

        # Make sure it realises all contacts should be further than the ID 0.
        self.assertTrue(len(router.further_contacts) == Constants.K,
                        "All contacts should be further.")

        self.assertTrue(len(router.closer_contacts) == 0, "Expected no closer contacts.")


    def test_z_lookup(self):

        for i in range(100):
            id = ID.random_id(seed=i)

            self.__setup()

            close_contacts: list[Contact] = self.router.lookup(
                key=id, rpc_call=self.router.rpc_find_nodes, give_me_all=True)["contacts"]

            contacted_nodes: list[Contact] = close_contacts
            self.get_alt_close_and_far(self.contacts_to_query,
                                       self.closer_contacts_alt_computation,
                                       self.further_contacts_alt_computation,
                                       self.nodes,
                                       key=id,
                                       distance=self.distance)

            self.assertTrue(len(close_contacts) >= len(self.closer_contacts_alt_computation),
                            f"Expected at least as many contacts: {len(close_contacts)} vs "
                            f"{len(self.closer_contacts_alt_computation)}")

            for c in self.closer_contacts_alt_computation:
                self.assertTrue(c in close_contacts,
                                "somehow a close contact in the computation is not in the originals?")


class LargeFileTests(unittest.TestCase):
    def test_large_file_splits(self):

        dht = DHT(ID.random_id(), VirtualProtocol(), storage_factory=VirtualStorage, router=Router())


if __name__ == '__main__':
    unittest.main()
