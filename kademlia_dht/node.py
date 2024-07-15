import logging

from kademlia_dht.buckets import BucketList
from kademlia_dht.constants import Constants
from kademlia_dht.contact import Contact
from kademlia_dht.dictionaries import CommonRequest
from kademlia_dht.errors import RPCError, SenderIsSelfError, SendingQueryToSelfError
from kademlia_dht.id import ID
from kademlia_dht.interfaces import IProtocol, IStorage
from kademlia_dht.storage import VirtualStorage

logger = logging.getLogger("__main__")


class Node:

    def __init__(self,
                 contact: Contact,
                 storage: IStorage,
                 cache_storage=None):
        """
        Initialises the node object. This object is used to represent a device on the
        network – contains IP, port, ID, storage, cache_storage, and the master DHT object controlling it.
        :param contact:
        :param storage:
        :param cache_storage:
        """

        if not cache_storage and not Constants.DEBUG:
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
        Stores a key-value pair in main/cache storage. This adds the sender to our bucket list – so it
        will error if the sender is ourselves. Then it will send key values if the contact is new
        (and not is_cached), then it will save the key-value pair in the corresponding storage object.

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
        Finds K close contacts to a given ID, whilst excluding the sender.
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
        contacts = self.bucket_list.get_close_contacts(key=key,
                                                       exclude=sender.id)
        return contacts, None

    def find_value(self, key: ID, sender: Contact) \
            -> tuple[list[Contact] | None, str | None]:
        """
        Sends key values if new contact, then attempts to find the value of a key-value pair in
        our storage (then cache storage), given the key. If it cannot do that, it will return
        K contacts that are closer to the key than it is.
        """
        if sender.id == self.our_contact.id:
            raise SendingQueryToSelfError("Sender cannot be ourselves.")

        self.send_key_values_if_new_contact(sender)

        if self.storage.contains(key):
            logger.debug(f" Value in self.storage of {self.our_contact.id}.")
            return None, self.storage.get(key)
        elif self.cache_storage.contains(key):
            if Constants.DEBUG:
                logger.debug(f"Value in self.cache_storage of {self.our_contact.id}.")
            return None, self.cache_storage.get(key)
        else:
            if Constants.DEBUG:
                logger.debug("Value not in storage, getting close contacts.")
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
        transfers a key-value pair if its own ID is closer to the key
        than are the IDs of other nodes."

        For a new contact, we store values to that contact whose keys
        XOR our_contact are less than the stored keys XOR other_contacts.
        """
        if self._is_new_contact(sender):
            # with self.bucket_list.lock:
            # Clone, so we can release the lock.
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
                        logger.debug(f"Protocol used by sender: {sender.protocol}")
                        error: RPCError | None = sender.protocol.store(
                            sender=self.our_contact,
                            key=ID(k),
                            val=self.storage.get(k)
                        )
                        if self.dht:
                            self.dht.handle_error(error, sender)

    def _is_new_contact(self, sender: Contact) -> bool:
        """
        Returns NOT(if the contact exists in our bucket list or in our DHT’s pending contact list.)
        :param sender:
        :return:
        """
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
        logger.info("[Server] Ping called")
        protocol: IProtocol = request["protocol"]
        self.ping(
            Contact(
                protocol=protocol,
                id=ID(request["sender"])
            )
        )
        return {"random_id": request["random_id"]}

    def server_store(self, request: CommonRequest) -> dict:
        logger.info("[Server] Server store called.")
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
        logger.info("[Server] Find node called")
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
        logger.info("[Server] Find Value called")
        protocol: IProtocol = request["protocol"]
        contacts, val = self.find_value(
            sender=Contact(
                protocol=protocol,
                id=ID(request["sender"])
            ),
            key=ID(request["key"])
        )
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
