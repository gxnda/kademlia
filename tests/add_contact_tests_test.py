import unittest

from ..kademlia import ID, BucketList, Constants, Contact, random_id_in_space


class addContactTests(unittest.TestCase):
    
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

        
        
    