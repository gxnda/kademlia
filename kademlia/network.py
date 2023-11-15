from datetime import datetime
from node import Node, ID



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


class KBucket:
    def __init__(self, k=20):
        """Initialises a k-bucket with a specific ID range, initially from 0 to 2**160."""
        self._contacts: list[Contact] = []
        self._low = 0
        self._high = 2**160
        self.time_stamp: datetime
        self.k = k

    def bucket_full(self):
        return len(self._contacts) >= self.k
    
    def touch(self):
        self.time_stamp = datetime.now()

    def is_in_range(self, contact: Contact):
        return self._low <= contact.id.value <= self._high
    
    def add_contact(self, contact: Contact):
        if self.bucket_full():
            raise TooManyContactsError("KBucket is full.")
        elif not self.is_in_range(contact):
            raise ValueError("Contact ID is out of range.")



if __name__ == "__main__":
    