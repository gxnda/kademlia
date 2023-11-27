from datetime import datetime
from node import ID, Node


class Router:

    def __init__(self, node: Node) -> None:
        self.node = node


class Contact:

    def __init__(self, contact_ID: ID, protocol=None):
        self.protocol = protocol
        self.id = contact_ID
        self.last_seen: datetime = datetime.now()

    def touch(self):
        """Updates the last time the contact was seen."""
        self.last_seen = datetime.now()


class TooManyContactsError(Exception):
    """Raised when a contact is added to a full k-bucket."""
    pass


class OutOfRangeError(Exception):
    """Raised when a contact is added to a k-bucket that is out of range."""
    pass


class KBucket:

    def __init__(self, k=20):
        """Initialises a k-bucket with a specific ID range, initially from 0 to 2**160."""
        self.contacts: list[Contact] = []
        self._low = 0
        self._high = 2**160
        self._time_stamp: datetime
        self._k = k

    def bucket_full(self):
        return len(self.contacts) >= self._k

    def touch(self):
        self.time_stamp = datetime.now()

    def is_in_range(self, contact: Contact):
        return self._low <= contact.id.value <= self._high

    def add_contact(self, contact: Contact):
        if self.bucket_full():
            raise TooManyContactsError("KBucket is full.")
        elif not self.is_in_range(contact):
            raise OutOfRangeError("Contact ID is out of range.")
        else:
            # !!! should this check whether or not the contact is already in the bucket?
            self.contacts.append(contact)

    def split(self):
        # !!! TO BE IMPLEMENTED
        pass

    def depth(self):
        for contact in self.contacts:
            id_bin = bin(contact.id.value)
            print(id_bin, id_bin[2:])


class BucketList:

    def __init__(self, our_id: ID):
        self.buckets: list[KBucket] = [KBucket()]
        # first k-bucket has max range
        self.our_id: ID = our_id

    def add_contact(self, contact: Contact) -> None:
        # !!! TO BE IMPLEMENTED
        pass
        


if __name__ == "__main__":
    bucket = KBucket()
    for i in range(1, 20):
        bucket.add_contact(Contact(ID(i)))
    print(bucket.contacts)
    bucket.depth()
