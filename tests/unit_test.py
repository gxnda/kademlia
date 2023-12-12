import unittest

from ..kademlia import ID, BucketList, Constants, Contact, random_id_in_space, Constants, Contact, ID, KBucket, TooManyContactsError, Node


class add_contact_test(unittest.TestCase):

    def unique_id_add_test(self):
        dummy_contact: Contact = Contact(VirtualProtocol(), ID(0))
        #  ((VirtualProtocol)dummyContact.Protocol).Node = new Node(dummyContact, new VirtualStorage());

        bucket_list: BucketList = BucketList(random_id_in_space(), dummy_contact) 

        for i in range(Constants().K):
            bucket_list.add_contact(Contact(random_id_in_space()))

        self.assertTrue(len(bucket_list.buckets) == 1, 
                         "No split should have taken place.")

        self.assertTrue(len(bucket_list.buckets[0].contacts) == Constants().K,
                        "K contacts should have been added.")

    def duplicate_id_test(self):
        dummy_contact = Contact(VirtualProtocol(), ID(0))
        #  ((VirtualProtocol)dummyContact.Protocol).Node = new Node(dummyContact, new VirtualStorage());
        bucket_list: BucketList = BucketList(random_id_in_space(), dummy_contact)

        id: ID = random_id_in_space()

        bucket_list.add_contact(Contact(id))
        bucket_list.add_contact(Contact(id))

        self.assertTrue(len(bucket_list.buckets) == 1, 
             "No split should have taken place.")

        self.assertTrue(len(bucket_list.buckets[0].contacts) == 1,
                       "Bucket should have one contact.")

    def bucket_split_test(self):

        dummy_contact = Contact(VirtualProtocol(), ID(0))
        #  ((VirtualProtocol)dummyContact.Protocol).Node = new Node(dummyContact, new VirtualStorage());
        bucket_list: BucketList = BucketList(random_id_in_space(), dummy_contact)
        for i in range(Constants().K):
            bucket_list.add_contact(Contact(random_id_in_space()))

        bucket_list.add_contact(Contact(random_id_in_space()))

        self.assertTrue(len(bucket_list.buckets) > 1,
                        "Bucket should have split into two or more buckets.")





class test_force_failed_add_test(unittest.TestCase):

    def test_force_failed_add(self):
        dummy_contact = Contact(contact_ID=ID(0))
        node = Node(contact=dummy_contact, storage=VirtualStorage());

        bucket_list: BucketList = setup_split_failure()
        
        assert(len(bucket_list.buckets) == 2, "Bucket split should have occured.")

        assert(len(bucket_list.buckets[0].contacts) == 1, "Expected 1 contact in bucket 0.")

        assert(len(bucket_list.buckets[1].contacts) == 20, "Expected 20 contacts in bucket 1.")

        # This next contact should not split the bucket as 
        # depth == 5 and therfore adding the contact will fail.

        # Any unique ID >= 2^159 will do.

        id = 2**159 + 4

        new_contact =  Contact(contact_ID=ID(id), protocol="dummy_contact.protocol")
        bucket_list.add_contact(new_contact)

        assert(len(bucket_list.buckets) == 2, "Bucket split should have occured.")

        assert(len(bucket_list.buckets[0].contacts) == 1, "Expected 1 contact in bucket 0.")

        assert(len(bucket_list.buckets[1].contacts) == 20, "Expected 20 contacts in bucket 1.")

        assert(new_contact not in bucket_list.buckets[1].contacts, "Expected new contact NOT to replace an older contact.")


class KBucketTests(unittest.TestCase):
    def test_too_many_contacts(self):
        with self.assertRaises(TooManyContactsError):
            K = Constants().K
            k_bucket = KBucket(k=K)
            for i in range(K):
                contact = Contact(ID(i))
                k_bucket.add_contact(contact)

            # Trying to add one more contact should raise the exception
            contact = Contact(ID(K + 1))
            k_bucket.add_contact(contact)


def get_close_contacts_ordered_test():
    sender: Contact = Contact(contact_ID=random_id_in_space(), protocol=None)
    node: Node = Node(Contact(contact_ID=random_id_in_space(), protocol=None), VirtualStorage())
    
    contacts: list[Contact] = []
    for _ in range(100):
        contacts.append(Contact(contact_ID=random_id_in_space(), protocol=None))
    
    key: ID = random_id_in_space()

    closest: list[Contact] = node.find_node(sender=sender, key=key)[0]

    assert(len(closest) == Constants().K, "Expected K contacts to be returned.")

    # the contacts should be in ascending order with respect to the key.
    distances: list[int] = [c.id ^ key for c in closest]
    distance: int = distances[0]

    for i in distances[1:]:
        assert(distance < i, "Expected contacts to be ordered by distance.")
        distance = i

    # Verify the contacts with the smallest distances have been returned from all possible distances.
    last_distance = distances[-1]
    others = []
    for b in node.bucket_list.buckets:
        for c in b.contacts:
            if c not in closest and (c.id ^ key) < last_distance:
                others.append(c)

    assert(len(others) == 0, "Expected no other contacts with a smaller distance than the greatest distance to exist.");
                
            
if __name__ == '__main__':
     unittest.main()
     print("Everything passed")
