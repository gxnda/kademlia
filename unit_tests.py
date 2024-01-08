import unittest

from kademlia import BucketList, VirtualProtocol, \
    Constants, Contact, ID, KBucket, TooManyContactsError, Node, \
    Router, VirtualStorage, DHT


def setup_split_failure(bucket_list = None):
    # force host node ID to < 2 ** 159 so the node ID is not in the
    # 2 ** 159 ... 2 ** 160 range.

    # May be incorrect - book does some weird byte manipulation.
    host_ID: ID = ID.random_id(2 ** 158, 2 ** 159 - 1)

    dummy_contact: Contact = Contact(host_ID, VirtualProtocol())
    dummy_contact.protocol.node = Node(dummy_contact, VirtualStorage())

    if not bucket_list:
        bucket_list = BucketList(our_id=host_ID, dummy_contact)

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
            int.from_bytes(b_contact_id, byteorder="big")
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

    def test_too_many_contacts(self):
        with self.assertRaises(TooManyContactsError):
            k = Constants.K
            k_bucket = KBucket()
            for i in range(k):
                contact = Contact(ID(i))
                k_bucket.add_contact(contact)

            # Trying to add one more contact should raise the exception
            contact = Contact(ID(k + 1))
            k_bucket.add_contact(contact)

    def test_no_funny_business(self):
        k1: KBucket = KBucket(low=0, high=100)
        k2: KBucket = KBucket(low=10, high=200, initial_contacts=[])
        self.assertTrue(k1.contacts == k2.contacts)


class AddContactTest(unittest.TestCase):

    def test_unique_id_add(self):
        dummy_contact: Contact = Contact(id=ID(0),
                                         protocol=VirtualProtocol())

        dummy_contact.protocol.node = Node(dummy_contact, VirtualStorage())

        bucket_list: BucketList = BucketList(ID.random_id())  # ,
                                             # dummy_contact)

        for i in range(Constants.K):
            bucket_list.add_contact(Contact(ID.random_id()))

        self.assertTrue(
            len(bucket_list.buckets) == 1, "No split should have taken place.")

        self.assertTrue(
            len(bucket_list.buckets[0].contacts) == Constants.K,
            "K contacts should have been added.")

    def test_duplicate_id(self):
        dummy_contact = Contact(ID(0), VirtualProtocol())
        #  ((VirtualProtocol)dummyContact.Protocol).Node = new Node(dummyContact, new VirtualStorage());
        bucket_list: BucketList = BucketList(ID.random_id())
        # !!! ^ There is a 2nd param "dummy_contact" in book here, 
        # book is rather silly sometimes imo.

        id: ID = ID.random_id()

        bucket_list.add_contact(Contact(id))
        bucket_list.add_contact(Contact(id))

        self.assertTrue(
            len(bucket_list.buckets) == 1, "No split should have taken place.")

        self.assertTrue(
            len(bucket_list.buckets[0].contacts) == 1,
            "Bucket should have one contact.")

    def test_bucket_split(self):

        # dummy_contact = Contact(VirtualProtocol(), ID(0))
        #  ((VirtualProtocol)dummyContact.Protocol).Node = new Node(dummyContact, new VirtualStorage());
        bucket_list: BucketList = BucketList(ID.random_id()) # ,
                                             # dummy_contact)
        for i in range(Constants.K):
            bucket_list.add_contact(Contact(ID.random_id()))
        bucket_list.add_contact(Contact(ID.random_id()))

        self.assertTrue(
            len(bucket_list.buckets) > 1,
            "Bucket should have split into two or more buckets. "
            f"Length of first buckets contacts = {len(bucket_list.buckets[0].contacts)}")


class ForceFailedAddTest(unittest.TestCase):
    """
    def test_force_failed_add(self):
        dummy_contact = Contact(contact_ID=ID(0))
        node = Node(contact=dummy_contact, storage=VirtualStorage())

        bucket_list: BucketList = setup_split_failure() # TODO: THIS FUNCTION DOES NOT EXIST.

        self.assertTrue(len(bucket_list.buckets) == 2,
                "Bucket split should have occured.")

        self.assertTrue(len(bucket_list.buckets[0].contacts) == 1,
                "Expected 1 contact in bucket 0.")

        self.assertTrue(len(bucket_list.buckets[1].contacts) == 20,
                "Expected 20 contacts in bucket 1.")

        # This next contact should not split the bucket as
        # depth == 5 and therfore adding the contact will fail.

        # Any unique ID >= 2^159 will do.

        id = 2**159 + 4

        new_contact = Contact(contact_ID=ID(id),
                              protocol="dummy_contact.protocol")
        bucket_list.add_contact(new_contact)

        self.assertTrue(len(bucket_list.buckets) == 2,
                "Bucket split should have occured.")

        self.assertTrue(len(bucket_list.buckets[0].contacts) == 1,
                "Expected 1 contact in bucket 0.")

        self.assertTrue(len(bucket_list.buckets[1].contacts) == 20,
                "Expected 20 contacts in bucket 1.")

        self.assertTrue(new_contact not in bucket_list.buckets[1].contacts,
                "Expected new contact NOT to replace an older contact.")
    """
    pass


class NodeLookupTests(unittest.TestCase):

    def test_get_close_contacts_ordered(self):
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
        # print(closest)
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
        last_distance = distances[-1]

        # This just makes sure it returned the K smallest contact ID's possible.
        others = []
        for b in node.bucket_list.buckets:
            for c in b.contacts:
                if c not in closest and (c.id ^ key) < last_distance:
                    others.append(c)

        self.assertTrue(
            len(others) == 0,
            "Expected no other contacts with a smaller distance than the greatest distance to exist."
        )

    def test_no_nodes_to_query(self):
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
            # nodes.ForEach(n => nodes.Where(nOther => nOther != n).ForEach(nOther => n.BucketList.AddContact(nOther.OurContact)));
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

        # TODO: Remove shell loops, just keeping them atm bc its how it is in book.
        
        for n in self.nodes:
            # fix protocols
            n.our_contact.protocol = VirtualProtocol(n)

        for n in self.nodes:
            # our contacts
            self.router.node.bucket_list.add_contact(n.our_contact)

        # let each peer know about all that are not themselves
        for n in self.nodes:
            for other_n in self.nodes:
                if other_n != n:
                    n.bucket_list.add_contact(other_n.our_contact)

        # pick a random bucket
        key = ID.random_id()
        # take "A" contacts from a random KBucket
        # TODO: Check this returns A contacts - also it could error if len(contacts) < A
        self.contacts_to_query: list[Contact] = \
            self.router.node.bucket_list.get_kbucket(key).contacts[:Constants.A]
        
        self.closer_contacts: list[Contact] = []
        self.further_contacts: list[Contact] = []

        self.closer_contacts_alt_computation: list[Contact] = []
        self.further_contacts_alt_computation: list[Contact] = []

        self.nearest_contact_node = sorted(self.contacts_to_query, key=lambda n: n.id ^ key)[0]
        self.distance = self.nearest_contact_node.id ^ key
        
    def get_alt_close_and_far(self, contacts_to_query: list[Contact], 
                              closer: list[Contact], 
                              further: list[Contact],
                              nodes: list[Node],
                              key: ID, # I think this is needed.
                              distance
                              ):
        """
        Alternate implementation for getting closer and further contacts.
        """
        # for each node in our bucket (nodes_to_query) we're going
        # to get k nodes closest to the key
        for contact in contacts_to_query:
            # very inefficient - TODO: Make more efficient.
            contact_node: Node = [i for i in nodes if i.our_contact == contact][0]
            # close contacts do not contain ourselves or the nodes we're contacting.
            # note that of all the contacts in the bucket list, many of the k returned
            # by the get_close_contacts call are contacts we're querying, so they are
            # being excluded.

            our_id = self.router.node.our_contact.id # our nodes ID

            # all close contacts to us, excluding ourselves
            temp_list = contact_node.bucket_list.get_close_contacts(key, our_id)
            close_contacts_of_contacted_node: list[Contact] = []

            # if we don't have to query the contact at some point, we can add it.
            for c in temp_list:
                if c.id not in [i.id for i in contacts_to_query]:
                    close_contacts_of_contacted_node.append(c)

            for close_contact in close_contacts_of_contacted_node:
                # work out which is closer, if it is closer, and we haven't already added it:
                if close_contact.id ^ key < distance and close_contact not in closer:
                    closer.append(close_contact)
                elif close_contact.id ^ key >= distance and close_contact not in further:
                    further.append(close_contact)
    
    def dont_test_lookup(self):


        for i in range(100):
            id = ID.random_id(seed=i)

            self.__setup()

            close_contacts: list[Contact] = self.router.lookup(
                key=id, rpc_call=self.router.rpc_find_nodes, give_me_all=True)[1]
            contacted_nodes: list[Contact] = close_contacts

            self.get_alt_close_and_far(self.contacts_to_query,
                                       self.closer_contacts_alt_computation,
                                       self.further_contacts_alt_computation,
                                       self.nodes,
                                       key=id, 
                                       distance=self.distance)

            self.assertTrue(len(close_contacts) >=
                    len(self.closer_contacts_alt_computation),
                    "Expected at least as many contacts.")
            for c in self.closer_contacts_alt_computation:
                self.assertTrue(c in close_contacts, 
                                "somehow a close contact in the computation is not in the originals?")

    def dont_test_simple_all_closer_contacts(self):
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
        contacts_to_query = router.node.bucket_list.buckets[0].contacts
        
        contacts = router.lookup(key=key, 
                                 rpc_call=router.rpc_find_nodes, 
                                 give_me_all=True)

        # Make sure lookup returns K contacts.
        self.assertTrue(len(contacts) == Constants.K, "Expected K closer contacts.")

        # Make sure it realises all contacts should be closer than 2**160 - 1.
        self.assertTrue(len(router.closer_contacts) == Constants.K,
                        "All contacts should be closer than the ID 2**160 - 1.")

        self.assertTrue(len(router.further_contacts) == 0, "Expected no further contacts.")

    def dont_test_simple_all_further_contacts(self):
        # setup
        # by selecting our node ID to zero, we ensure that all distances of other nodes 
        # are greater than the distance to our node.

        # Create a router with the largest ID possible.
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
    
        # select the key such that n ^ 0 == n (TODO: Why?)
        # this ensures the distance metric uses only the node ID,
        # which makes for an integer difference for distance, not an XOR distance.
        key = ID(0)
        # all contacts are in one bucket
        # This is because we added K node's contacts to router,
        # so it shouldn't have split.
        contacts_to_query = router.node.bucket_list.buckets[0].contacts
    
        contacts = router.lookup(key=key, 
                                 rpc_call=router.rpc_find_nodes, 
                                 give_me_all=True)
    
        # Make sure lookup returns K contacts.
        self.assertTrue(len(contacts) == Constants.K, "Expected K closer contacts.")
    
        # Make sure it realises all contacts should be further than the ID 0.
        self.assertTrue(len(router.further_contacts) == Constants.K,
                        "All contacts should be further than the ID 0.")
    
        self.assertTrue(len(router.closer_contacts) == 0, "Expected no closer contacts.")


class DHTTest(unittest.TestCase):
    def test_local_store_find_value(self):
        vp = VirtualProtocol()
        # Below line should contain VirtualStorage(), which I don't have?
        dht = DHT(id=ID.random_id(),
                  protocol=vp,
                  storage_factory=VirtualStorage,
                  router=Router())
        vp.node = dht._router.node
        key = ID.random_id()
        dht.store(key, "Test")
        return_val = dht.find_value(key)

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
        # TODO: Why are there 6 arguments? im tired i sleep now
        dht = DHT(id=ID.max(), router=Router(), storage_factory=lambda: store1, protocol=VirtualProtocol())

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

        retval: str = dht.find_value(key)[2]
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

        dht: DHT = DHT(id=ID.max(), protocol=vp1, router=Router(), storage_factory=lambda: store1)

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
        other_node_2._storage.set(key, val)

        # setup node 3
        contact_id_3 = ID(2 ** 158)  # I think this is the same as ID.Zero.SetBit(158)?
        other_contact_3 = Contact(contact_id_3, vp3)
        other_node_3 = Node(other_contact_3, store3, cache_storage=cache3)
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


class BootstrappingTests(unittest.TestCase):

    def test_random_within_bucket_tests(self):

        test_cases: list[tuple[int, int]] = [

                (0, 256),              # 7 bits should be set
                (256, 1024),          # 2 bits (256 + 512) should be set
                (65536, 65536 * 2),   # no additional bits should be set.
                (65536, 65536 * 4),   # 2 bits (65536 and 65536*2) should be set.
                (65536, 65536 * 16)   # 4 bits (65536, 65536*2, 65536*4, 65536*8) set.
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
        dht_us: DHT = DHT(ID.random_id(), vp[0], VirtualStorage, Router())
        vp[0].node = dht_us._router.node

        # our bootstrap peer
        dht_bootstrap: DHT = DHT(ID.random_id(), vp[1], VirtualStorage, Router())
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
        dht_us: DHT = DHT(ID.random_id(), vp[0], VirtualStorage, Router())
        vp[0].node = dht_us._router.node

        # our bootstrap peer
        # all IDs are < 2 ** 159
        dht_bootstrap: DHT = DHT(ID.random_id(0, 2 ** 159 - 1), vp[1], VirtualStorage, Router())
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

        self.assertTrue(len(n.bucket_list.contacts()) == 10, f"contacts: {len(n.bucket_list.contacts())}")

        self.assertTrue(len(important_contact.protocol.node.bucket_list.contacts()) == 10, f"contacts: {len(n.bucket_list.contacts())}")

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


        print("Starting bootstrap...")
        dht_us.bootstrap(dht_bootstrap._router.node.our_contact)
        print("Bootstrap finished!")

        print(f"\nLength of buckets: {[len(b.contacts) for b in dht_us._router.node.bucket_list.buckets]}")

        sum_of_contacts = len(dht_us._router.node.bucket_list.contacts())
        self.assertTrue(sum_of_contacts == 31,
                        f"Expected our peer to have 31 contacts, {sum_of_contacts} were given.")


class BucketManagementTests(unittest.TestCase):
    def test_non_responding_contact_evicted(self):
        """
        Tests that a nonresponding contact is evicted after 
        Constants.EVICTION_LIMIT tries.
        """
        dht = DHT(ID(0), VirtualProtocol(), None, Router())
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

        next_new_contact = Contact(
            ID(2 ** 159),
            dht.our_contact.protocol
        )

        # Hit the nonresponding contact EVICTION_LIMIT times
        # Which will trigger the eviction algorithm.

        for _ in range(Constants.EVICTION_LIMIT):
            bucket_list.add_contact(non_responding_contact)
        
        self.assertTrue(len(bucket_list.buckets[1].contacts) == 20, 
                        "Expected 20 contacts in bucket 1.")

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
        dht = DHT(ID(0), VirtualProtocol(), None, Router())
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
            id=ID(2 ** 159),
            protocol=dht.our_contact.protocol
        )
        bucket_list.add_contact(next_new_contact)

        self.assertTrue(len(bucket_list.buckets[1].contacts) == 20,
                       "Expecting 20 contacts in bucket 1."
                       )
        # Verify can_split -> Evict happened.
        self.assertTrue(len(dht.pending_contacts) == 1,
                        "Expected one pending contact.")
        self.assertTrue(next_new_contact in dht.pending_contacts, 
                       "Expected pending contact to be the 21st contact.")
        self.assertTrue(len(dht.eviction_count) == 1, 
                        "Expected one contact to be pending eviction.")

        
        
if __name__ == '__main__':
    unittest.main()
