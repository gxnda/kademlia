from datetime import datetime
from typing import Optional

from kademlia_dht.constants import Constants
from kademlia_dht.id import ID
from kademlia_dht.interfaces import IProtocol


class Contact:

    def __init__(self, id: ID, protocol=None):
        if protocol is None and not Constants.DEBUG:
            raise ValueError("No protocol given to Contact.")
        self.protocol: Optional[IProtocol] = protocol
        self.id = id
        self.last_seen: datetime = datetime.now()

    def touch(self) -> None:
        """Updates the last time the contact was seen."""
        self.last_seen = datetime.now()
