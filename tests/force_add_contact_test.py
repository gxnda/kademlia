import unittest

from ..kademlia import BucketList, Contact, ID, BucketList, Node


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


    