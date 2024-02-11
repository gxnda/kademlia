from kademlia.buckets import BucketList
from kademlia.constants import Constants
from kademlia.contact import Contact
from kademlia.dictionaries import CommonRequest
from kademlia.errors import RPCError, SenderIsSelfError, SendingQueryToSelfError
from kademlia.id import ID
from kademlia.interfaces import IProtocol, IStorage
from kademlia.main import DEBUG
from kademlia.storage import VirtualStorage


class Node:

    def __init__(self,
                 contact: Contact,
                 storage: IStorage,
                 cache_storage=None):

        if not cache_storage and not DEBUG:
            raise ValueError(
                "cache_storage must be supplied to type node if debug mode is not enabled."
            )

        self.our_contact: Contact = contact
        self.storage: IStorage = storage

        # VirtualStorage will only be created by
        self.cache_storage: IStorage = cache_storage if cache_storage else VirtualStorage()
        self.dht = None  # This should never be None
        self.bucket_list = BucketList(contact)

    def ping(self, sender: Contact) -> Contact:
        """
        Someone is pinging us.
        Register the contact and respond with our contact.
        """
        if sender.id.value == self.our_contact.id.value:
            raise SendingQueryToSelfError(
                "Sender of ping RPC cannot be ourself."
            )
        self.send_key_values_if_new_contact(sender)
        self.bucket_list.add_contact(sender)

        return self.our_contact

    def store(self,
              key: ID,
              sender: Contact,
              val: str,
              is_cached: bool = False,
              expiration_time_sec: int = 0) -> None:
        """
        Store a key-value pair in the republish or cache storage.
        :param key:
        :param sender:
        :param val:
        :param is_cached:
        :param expiration_time_sec:
        :return:
        """

        if sender.id.value == self.our_contact.id.value:
            raise SenderIsSelfError("Sender should not be ourself.")

        # add sender to bucket_list (updating bucket list like how it is in spec.)
        self.bucket_list.add_contact(sender)

        if is_cached:
            self.cache_storage.set(key, val, expiration_time_sec)
        else:
            self.send_key_values_if_new_contact(sender)
            self.storage.set(key, val, Constants.EXPIRATION_TIME_SEC)

    def find_node(self, key: ID,
                  sender: Contact) -> tuple[list[Contact], str | None]:
        """
        Finds K close contacts to a given ID, while exluding the sender.
        It also adds the sender if it hasn't seen it before.
        :param key: K close contacts are found near this ID.
        :param sender: Contact to be excluded and added if new.
        :return: list of K (or less) contacts near the key
        """

        # managing sender
        if sender.id == self.our_contact.id:
            raise SendingQueryToSelfError("Sender cannot be ourselves.")
        self.send_key_values_if_new_contact(sender)
        self.bucket_list.add_contact(sender)

        # actually finding nodes
        # print([len(b.contacts) for b in self.bucket_list.buckets])
        contacts = self.bucket_list.get_close_contacts(key=key,
                                                       exclude=sender.id)
        # print(f"contacts: {contacts}")
        return contacts, None

    def find_value(self, key: ID, sender: Contact) \
            -> tuple[list[Contact] | None, str | None]:
        """
        Find value in self.storage, testing
        self.cache_storage if it is not in the former.
        If it is not in either, it gets K
        close contacts from the bucket list.
        """
        print("find_value called")
        if sender.id == self.our_contact.id:
            raise SendingQueryToSelfError("Sender cannot be ourselves.")
        self.send_key_values_if_new_contact(sender)
        if self.storage.contains(key):
            print(f"[DEBUG] Value in self.storage of {self.our_contact.id}.")
            return None, self.storage.get(key)
        elif self.cache_storage.contains(key):
            print(f"[DEBUG] Value in self.cache_storage of {self.our_contact.id}.")
            return None, self.cache_storage.get(key)
        else:
            print("[DEBUG] Value not in storage, getting close contacts.")
            return self.bucket_list.get_close_contacts(key, sender.id), None

    def send_key_values_if_new_contact(self, sender: Contact) -> None:
        """
        Spec: "When a new node joins the system, it must store any
        key-value pair to which it is one of the k closest. Existing
        nodes, by similarly exploiting complete knowledge of their
        surrounding subtrees, will know which key-value pairs the new
        node should store. Any node learning of a new node therefore
        issues STORE RPCs to transfer relevant key-value pairs to the
        new node. To avoid redundant STORE RPCs, however, a node only
        transfers a key-value pair if it’s own ID is closer to the key
        than are the IDs of other nodes."

        For a new contact, we store values to that contact whose keys
        XOR our_contact are less than the stored keys XOR other_contacts.
        """
        # print("send key values if new contact")
        if self._is_new_contact(sender):
            # with self.bucket_list.lock:
            # Clone so we can release the lock.
            contacts: list[Contact] = self.bucket_list.contacts()
            if len(contacts) > 0:
                # and our distance to the key < any other contact's distance
                # to the key
                for k in self.storage.get_keys():
                    # our minimum distance to the contact.
                    distance = min([c.id ^ k for c in contacts])
                    # If our contact is closer, store the contact on its
                    # node.
                    if (self.our_contact.id ^ k) < distance:
                        print(sender.protocol)
                        error: RPCError | None = sender.protocol.store(
                            sender=self.our_contact,
                            key=ID(k),
                            val=self.storage.get(k)
                        )
                        if self.dht:
                            self.dht.handle_error(error, sender)

    def _is_new_contact(self, sender: Contact) -> bool:
        ret: bool
        # with self.bucket_list.lock:
        ret: bool = self.bucket_list.contact_exists(sender)
        # end lock
        if self.dht:  # might be None in unit testing
            # with self.DHT.pending_contacts.lock:
            ret |= (sender.id in [c.id for c in self.dht.pending_contacts])
            # end lock

        return not ret

    def simply_store(self, key, val) -> None:
        """
        For unit testing.
        :param key:
        :param val:
        :return: None
        """
        self.storage.set(key, val)

    # Server entry points

    def server_ping(self, request: CommonRequest) -> dict:
        protocol: IProtocol = request["protocol"]
        self.ping(
            Contact(
                protocol=protocol,
                id=ID(request["sender"])
            )
        )
        return {"random_id": request["random_id"]}

    def server_store(self, request: CommonRequest) -> dict:
        print("[Server] Server store called.")
        protocol: IProtocol = request["protocol"]
        self.store(
            sender=Contact(
                id=ID(request["sender"]),
                protocol=protocol
            ),
            key=ID(request["key"]),
            val=str(request["value"]),
            is_cached=request["is_cached"],
            expiration_time_sec=request["expiration_time_sec"]
        )
        return {"random_id": request["random_id"]}

    def server_find_node(self, request: CommonRequest) -> dict:
        protocol: IProtocol = request["protocol"]

        contacts, val = self.find_node(
            sender=Contact(
                protocol=protocol,
                id=ID(request["sender"])
            ),
            key=ID(request["key"])
        )

        contact_dict: list[dict] = []
        for c in contacts:
            contact_info = {
                "contact": c.id.value,
                "protocol": c.protocol,
                "protocol_name": type(c.protocol)
            }

            contact_dict.append(contact_info)

        return {"contacts": contact_dict, "random_id": request["random_id"]}

    def server_find_value(self, request: CommonRequest) -> dict:
        print("[Server] Find Value called")
        protocol: IProtocol = request["protocol"]
        print(protocol)
        contacts, val = self.find_value(
            sender=Contact(
                protocol=protocol,
                id=ID(request["sender"])
            ),
            key=ID(request["key"])
        )
        print(contacts, val)
        contact_dict: list[dict] = []
        if contacts:
            for c in contacts:
                contact_info = {
                    "contact": c.id.value,
                    "protocol": c.protocol,
                    "protocol_name": type(c.protocol)
                }
                contact_dict.append(contact_info)
        return {"contacts": contact_dict,
                "random_id": request["random_id"],
                "value": val}
