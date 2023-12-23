import unittest

from kademlia import ID, BucketList, Constants, Contact, VirtualProtocol, random_id_in_space, Constants, Contact, ID, KBucket, TooManyContactsError, Node, Router, VirtualStorage


class test_KBucket(unittest.TestCase):

    def test_too_many_contacts(self):
        with self.assertRaises(TooManyContactsError):
            K = Constants().K
            k_bucket = KBucket()
            for i in range(K):
                contact = Contact(ID(i))
                k_bucket.add_contact(contact)

            # Trying to add one more contact should raise the exception
            contact = Contact(ID(K + 1))
            k_bucket.add_contact(contact)


class test_add_contact(unittest.TestCase):

    def test_unique_id_add_test(self):
        dummy_contact: Contact = Contact(contact_ID=ID(0),
                                         protocol=VirtualProtocol())

        dummy_contact.protocol.node = Node(dummy_contact, VirtualStorage())

        bucket_list: BucketList = BucketList(random_id_in_space())  # ,
                                             # dummy_contact)

        for i in range(Constants().K):
            bucket_list.add_contact(Contact(random_id_in_space()))

        self.assertTrue(
            len(bucket_list.buckets) == 1, "No split should have taken place.")

        self.assertTrue(
            len(bucket_list.buckets[0].contacts) == Constants().K,
            "K contacts should have been added.")

    def duplicate_id_test(self):
        dummy_contact = Contact(ID(0), VirtualProtocol())
        #  ((VirtualProtocol)dummyContact.Protocol).Node = new Node(dummyContact, new VirtualStorage());
        bucket_list: BucketList = BucketList(random_id_in_space()) 
        # !!! ^ There is a 2nd param "dummy_contact" in book here, 
        # book is rather silly sometimes imo.

        id: ID = random_id_in_space()

        bucket_list.add_contact(Contact(id))
        bucket_list.add_contact(Contact(id))

        self.assertTrue(
            len(bucket_list.buckets) == 1, "No split should have taken place.")

        self.assertTrue(
            len(bucket_list.buckets[0].contacts) == 1,
            "Bucket should have one contact.")

    def test_bucket_split_test(self):

        # dummy_contact = Contact(VirtualProtocol(), ID(0))
        #  ((VirtualProtocol)dummyContact.Protocol).Node = new Node(dummyContact, new VirtualStorage());
        bucket_list: BucketList = BucketList(random_id_in_space()) # ,
                                             # dummy_contact)
        for i in range(Constants().K):
            bucket_list.add_contact(Contact(random_id_in_space()))
        bucket_list.add_contact(Contact(random_id_in_space()))

        self.assertTrue(
            len(bucket_list.buckets) > 1,
            "Bucket should have split into two or more buckets. "
            f"Length of first buckets contacts = {len(bucket_list.buckets[0].contacts)}")


class test_force_failed_add_test(unittest.TestCase):
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

    def get_close_contacts_ordered_test(self):
        sender: Contact = Contact(contact_ID=random_id_in_space(),
                                  protocol=None)
        node: Node = Node(
            Contact(contact_ID=random_id_in_space(), protocol=None),
            VirtualStorage())

        contacts: list[Contact] = []
        for _ in range(100):
            contacts.append(
                Contact(contact_ID=random_id_in_space(), protocol=None))

        key: ID = random_id_in_space()

        closest: list[Contact] = node.find_node(sender=sender, key=key)[0]

        self.assertTrue(len(closest) == Constants().K,
                "Expected K contacts to be returned.")

        # the contacts should be in ascending order with respect to the key.
        distances: list[int] = [c.id ^ key for c in closest]
        distance: int = distances[0]

        for i in distances[1:]:
            self.assertTrue(distance < i,
                    "Expected contacts to be ordered by distance.")
            distance = i

        # Verify the contacts with the smallest distances have been returned from all possible distances.
        last_distance = distances[-1]
        others = []
        for b in node.bucket_list.buckets:
            for c in b.contacts:
                if c not in closest and (c.id ^ key) < last_distance:
                    others.append(c)

        self.assertTrue(
            len(others) == 0,
            "Expected no other contacts with a smaller distance than the greatest distance to exist."
        )

    def no_nodes_to_query_test(self):
        router_node_contact = Contact(contact_ID=random_id_in_space(),
                                      protocol=None)
        router = Router(
            node=Node(contact=router_node_contact, storage=VirtualStorage()))

        nodes: list[Node] = []

        for i in range(Constants().K):
            nodes.append(
                Node(Contact(contact_ID=ID(2**i)), storage=VirtualStorage()))

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
        router = Router(
            Node(Contact(contact_ID=random_id_in_space(), protocol=None),
                 storage=VirtualStorage()))

        nodes: list[Node] = []
        for _ in range(100):
            contact: Contact = Contact(contact_ID=random_id_in_space(), protocol=VirtualProtocol())
            node: Node = Node(contact, VirtualStorage())
            contact.protocol.node = node
            nodes.append(node)

        # TODO: Remove shell loops, just keeping them atm bc its how it is in book.
        
        for n in nodes:
            # fix protocols
            n.our_contact.protocol = VirtualProtocol(n)

        for n in nodes:
            # our contacts
            router.node.bucket_list.add_contact(n.our_contact)

        # let each peer know about all that are not themselves
        for n in nodes:
            for other_n in nodes:
                if other_n != n:
                    n.bucket_list.add_contact(other_n.our_contact)

        # pick a random bucket
        key = random_id_in_space()
        # take "A" contacts from a random KBucket
        # TODO: Check this returns A contacts - also it could error if len(contacts) < A
        contacts_to_query: list[Contact] = \
            router.node.bucket_list.get_kbucket(key).contacts[:Constants().A]
        
        closer_contacts: list[Contact] = []
        further_contacts: list[Contact] = []

        closer_contacts_alt_computation: list[Contact] = []
        further_contacts_alt_computation: list[Contact] = []

        nearest_contact_node = sorted(contacts_to_query, key=lambda n: n.id ^ key)[0]
        distance = nearest_contact_node.id ^ key

        return router, nodes, contacts_to_query, closer_contacts, \
                further_contacts, closer_contacts_alt_computation, \
                further_contacts_alt_computation, nearest_contact_node, \
                distance
        
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

            our_id = router.node.our_contact.id # our nodes ID

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







    
    def lookup_test(self):


        for i in range(100):
            id = random_id_in_space(seed=i)

            # I'm so sorry
            # TODO: Make this bearable to look at
            router, nodes, contacts_to_query, closer_contacts, further_contacts, \
            closer_contacts_alt_computation, further_contacts_alt_computation, \
            nearest_contact_node, distance = self.__setup()
            
            close_contacts: list[Contact] = router.lookup(
                key=id, rpc_call=router.rpc_find_nodes, give_me_all=True)[1]
            contacted_nodes: list[Contact] = close_contacts

            self.get_alt_close_and_far(contacts_to_query,
                                       closer_contacts_alt_computation,
                                       further_contacts_alt_computation, 
                                       nodes, 
                                       key=id, 
                                       distance=distance)

            self.assertTrue(len(close_contacts) >=
                    len(closer_contacts_alt_computation),
                    "Expected at least as many contacts.")
            for c in closer_contacts_alt_computation:
                self.assertTrue(c in close_contacts, 
                                "somehow a close contact in the computation is not in the originals?")

    def simple_all_closer_contacts_test(self):
        # setup
        # by selecting our node ID to zero, we ensure that all distances of other nodes 
        # are greater than the distance to our node.

        # Create a router with the largest ID possible.
        router = Router(Node(Contact(contact_ID=ID(2**160 - 1), protocol=None), VirtualStorage()))
        nodes: list[Node] = []

        for n in range(Constants().K):
            # Create a node with id of a power of 2, up to 2**20.
            node = Node(Contact(contact_ID=ID(2**n), protocol=None), storage=VirtualStorage())
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
        self.assertTrue(len(contacts) == Constants().K, "Expected K closer contacts.")

        # Make sure it realises all contacts should be closer than 2**160 - 1.
        self.assertTrue(len(router.closer_contacts) == Constants().K,
                        "All contacts should be closer than the ID 2**160 - 1.")

        self.assertTrue(len(router.further_contacts) == 0, "Expected no further contacts.")

    def simple_all_closer_contacts_test(self):
        # setup
        # by selecting our node ID to zero, we ensure that all distances of other nodes 
        # are greater than the distance to our node.
    
        # Create a router with the largest ID possible.
        router = Router(Node(Contact(contact_ID=ID(0), protocol=None), VirtualStorage()))
        nodes: list[Node] = []
    
        for n in range(Constants().K):
            # Create a node with id of a power of 2, up to 2**20.
            node = Node(Contact(contact_ID=ID(2**n), protocol=None), storage=VirtualStorage())
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
        self.assertTrue(len(contacts) == Constants().K, "Expected K closer contacts.")
    
        # Make sure it realises all contacts should be further than the ID 0.
        self.assertTrue(len(router.further_contacts) == Constants().K,
                        "All contacts should be further than the ID 0.")
    
        self.assertTrue(len(router.closer_contacts) == 0, "Expected no closer contacts.")
    
        


if __name__ == '__main__':
    unittest.main()
