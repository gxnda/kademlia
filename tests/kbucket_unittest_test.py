import unittest

from ..kademlia.kademlia import Constants, Contact, ID, KBucket, TooManyContactsError


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


if __name__ == '__main__':
     unittest.main()
     print("Everything passed")
