import requests
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric.x25519 import X25519PrivateKey
from cryptography.hazmat.primitives.kdf.hkdf import HKDF

from kademlia import pickler
from kademlia.constants import Constants
from kademlia.contact import Contact
from kademlia.dictionaries import (BaseResponse, ErrorResponse, FindNodeSubnetRequest,
                                   FindValueSubnetRequest, PingSubnetRequest, StoreSubnetRequest, FindNodeRequest,
                                   FindValueRequest, PingRequest, StoreRequest)
from kademlia.errors import RPCError
from kademlia.id import ID
from kademlia.interfaces import IProtocol
from kademlia.node import Node
from kademlia.pickler import encrypt_data


def get_rpc_error(id: ID,
                  ret: BaseResponse | None,
                  timeout_error: bool,
                  peer_error: ErrorResponse) -> RPCError:
    print("Peer error:", peer_error)
    error = RPCError()
    if ret:
        error.id_mismatch_error = id != ret["random_id"]
    else:
        error.id_mismatch_error = False
    error.timeout_error = timeout_error
    error.peer_error = peer_error["error_message"] not in ["", None]
    if peer_error["error_message"]:
        error.peer_error_message = peer_error["error_message"]

    return error


class VirtualProtocol(IProtocol):
    """
    For unit testing, doesn't really do much in the main
    implementation, it's just used to make sure everything that
    doesn't involve networking works correctly.
    """

    def __init__(self, node: Node | None = None, responds=True) -> None:
        self.responds = responds
        self.node = node
        self.type = "VirtualProtocol"

    def ping(self, sender: Contact) -> RPCError | None:
        if self.responds:
            self.node.ping(sender)
        else:
            error = RPCError(
                "Time out while pinging contact - VirtualProtocol does not respond.",
                timeout_error=not self.responds
            )
            return error

    def find_node(self, sender: Contact,
                  key: ID) -> tuple[list[Contact], RPCError | None]:
        """
        Finds K close contacts to a given ID, while excluding the sender.
        It also adds the sender if it hasn't seen it before.
        :param key: K close contacts are found near this ID.
        :param sender: Contact to be excluded and added if new.
        :return: list of K (or less) contacts near the key, and an error that may need to be handled.
        """
        return self.node.find_node(sender=sender, key=key)[0], None

    def find_value(self, sender: Contact,
                   key: ID) -> tuple[list[Contact] | None, str | None, RPCError | None]:
        """
        Returns either contacts or None if the value is found.
        """
        contacts, val = self.node.find_value(sender=sender, key=key)
        return contacts, val, None

    def store(self,
              sender: Contact,
              key: ID,
              val: str,
              is_cached=False,
              exp_time: int = 0) -> RPCError | None:
        """
        Stores the key-value on the remote peer.
        """
        self.node.store(sender=sender,
                        key=key,
                        val=val,
                        is_cached=is_cached,
                        expiration_time_sec=exp_time)

        return None


class TCPSubnetProtocol(IProtocol):

    def __init__(self, url: str, port: int, subnet: int):
        self.url = url
        self.port = port
        self.responds = True
        self.subnet = subnet
        self.type = "TCPSubnetProtocol"
        # Generate a private key for use in the exchange
        self.__private_key = X25519PrivateKey.generate()
        self.peer_public_key = X25519PrivateKey.generate().public_key()

    def find_node(self, sender: Contact, key: ID) -> tuple[list[Contact] | None, RPCError]:
        id: ID = ID.random_id()

        encoded_data = encrypt_data(
            dict(FindNodeSubnetRequest(
                protocol=sender.protocol,
                protocol_name=type(sender.protocol),
                subnet=self.subnet,
                sender=sender.id.value,
                key=key.value,
                random_id=id.value
            ))
        )
        print(f"http://{self.url}:{self.port}/find_node")

        ret = None
        timeout_error = False
        error = ""
        try:
            print("[Client] Sending find_node RPC...")
            ret = requests.post(
                f"http://{self.url}:{self.port}/find_node",
                data=encoded_data,
                timeout=Constants.REQUEST_TIMEOUT
            )
            print(ret)

        except requests.Timeout as t:
            print("[ERROR] [Client] Timeout error when contacting node.\n", t)
            timeout_error = True
            error = t

        except Exception as e:
            print("[ERROR] [Client]", e)
            # request timed out.
            timeout_error = False
            error = e

        if ret:
            encoded_data = ret.content
            ret_decoded = pickler.decrypt_data(encoded_data)
        else:
            ret_decoded = None
        try:
            if ret_decoded:
                if ret_decoded["contacts"]:
                    contacts = []
                    for val in ret_decoded["contacts"]:
                        new_c = Contact(ID(val["contact"]), val["protocol"])
                        contacts.append(new_c)
                    # Return only contacts with supported protocols.
                    rpc_error = get_rpc_error(id,
                                              ret_decoded,
                                              timeout_error,
                                              ErrorResponse(error_message=str(error), random_id=ID.random_id()))
                    if contacts:
                        ret_contacts = [c for c in contacts if c.protocol is not None]
                        return ret_contacts, rpc_error
            else:
                rpc_error = get_rpc_error(id,
                                          ret_decoded,
                                          timeout_error,
                                          ErrorResponse(error_message=str(error), random_id=ID.random_id()))
                return [], rpc_error
        except Exception as e:
            error = RPCError()
            error.protocol_error = True
            print("[Client] Exception thrown: ", e)
            return None, error

    def find_value(self, sender: Contact, key: ID) -> tuple[list[Contact] | None, str | None, RPCError | None]:
        """
        Attempt to find the value in the peer network.

        A null contact list is acceptable as it is a valid return
        if the value is found.
        The caller is responsible for checking the timeoutError flag
        to make sure null contacts is not the result of a timeout
        error.

        :param sender: Sender to find value from
        :param key: Key to check for value from key-value pair
        :return: contacts, value, RPCError
        """
        random_id = ID.random_id()
        encoded_data = encrypt_data(
            dict(FindValueSubnetRequest(
                protocol=sender.protocol,
                protocol_name=type(sender.protocol),
                subnet=self.subnet,
                sender=sender.id.value,
                key=key.value,
                random_id=random_id.value
            ))
        )

        ret = None
        try:
            print("[Client] Sending POST")
            ret = requests.post(
                url=f"http://{self.url}:{self.port}/find_value",
                data=encoded_data,
                timeout=Constants.REQUEST_TIMEOUT
            )
            print("[Client] Completed POST")
            timeout_error = False
            error = None

        except requests.Timeout as t:
            print("Timeout error", t)
            timeout_error = True
            error = t

        except Exception as e:
            print("Exception!", e)
            # request timed out.
            timeout_error = False
            error = e

        ret_decoded = None
        if ret:
            encoded_data = ret.content
            ret_decoded = pickler.decrypt_data(encoded_data)

        try:
            contacts = []
            if ret_decoded:
                if ret_decoded["contacts"]:
                    for c in ret_decoded["contacts"]:
                        new_contact = Contact(
                            c["protocol"],  # instantiate_protocol
                            ID(c["contact"])
                        )
                        contacts.append(new_contact)
                        print("about to return")

                return [c for c in contacts if c.protocol is not None], \
                    ret_decoded["value"], \
                    get_rpc_error(
                        random_id, ret_decoded, timeout_error, ErrorResponse(
                            random_id=random_id.value,
                            error_message=str(error))
                    )
            else:
                return [c for c in contacts if c.protocol is not None], "", get_rpc_error(
                        random_id, ret_decoded, timeout_error, ErrorResponse(
                            random_id=random_id.value,
                            error_message=str(error))
                    )
        except Exception as e:
            rpc_error = RPCError(str(e))
            rpc_error.protocol_error = True
            print(f"[Client] Error performing find_value: {rpc_error}")
            return None, None, rpc_error

    def ping(self, sender: Contact) -> RPCError:
        random_id = ID.random_id()
        encoded_data = encrypt_data(
            dict(PingSubnetRequest(
                protocol=sender.protocol,
                protocol_name=type(sender.protocol),
                subnet=self.subnet,
                sender=sender.id.value,
                random_id=random_id.value)))

        timeout_error = False
        error = None
        ret = None
        try:
            print("[Client] Sending Ping RPC...")
            ret: requests.Response = requests.post(
                url=f"http://{self.url}:{self.port}/ping",
                data=encoded_data,
                timeout=Constants.REQUEST_TIMEOUT
            )
            print(f"[Client] Received HTTP Response from {ret.url} with code {ret.status_code}")

        except requests.Timeout as t:
            print("[Client] Ping timeout error: ", t)
            timeout_error = True
            error = t

        except Exception as e:
            print("[ERROR] [Client] Other exception thrown (Ping): ", e)
            # request timed out.
            timeout_error = False
            error = e

        ret_base_response = None

        formatted_response = None
        if ret:
            encoded_data = ret.content
            formatted_response = pickler.decrypt_data(encoded_data)

        return get_rpc_error(random_id, formatted_response, timeout_error, ErrorResponse(
            error_message=str(error), random_id=ID.random_id()))

    def store(self,
              sender: Contact,
              key: ID,
              val: str,
              is_cached=False,
              expiration_time_sec=0
              ) -> RPCError:
        random_id = ID.random_id()

        encoded_data = encrypt_data(
            dict(StoreSubnetRequest(
                protocol=sender.protocol,
                protocol_name=type(sender.protocol),
                subnet=self.subnet,
                sender=sender.id.value,
                key=key.value,
                value=val,
                is_cached=is_cached,
                expiration_time_sec=expiration_time_sec,
                random_id=random_id.value)))

        timeout_error = False
        error = None
        ret = None

        try:
            print(f"[Client] Running Store POST to http://{self.url}:{self.port}/store")
            ret = requests.post(
                url=f"http://{self.url}:{self.port}/store",
                data=encoded_data,
                timeout=Constants.REQUEST_TIMEOUT
            )
            print("[Client] Store POST done!")

        except requests.Timeout as t:
            print("[Client] Timeout error when contacting node.")
            timeout_error = True
            error = t

        except Exception as e:
            print("Exception!", e)
            # request timed out.
            timeout_error = False
            error = e

        # if ret.status_code == 200:
        # TODO: Add error handling

        formatted_response = None
        if ret:
            encoded_data = ret.content
            formatted_response = pickler.decrypt_data(encoded_data)

        return get_rpc_error(random_id, formatted_response, timeout_error, ErrorResponse(
            error_message=str(error), random_id=ID.random_id()))


class TCPProtocol(IProtocol):

    def __init__(self, url: str, port: int):
        self.url = url
        self.port = port
        self.responds = True
        self.type = "TCPSubnetProtocol"

    def find_node(self, sender: Contact, key: ID) -> tuple[list[Contact] | None, RPCError]:
        id: ID = ID.random_id()
        encoded_data = encrypt_data(
            dict(FindNodeRequest(
                protocol=sender.protocol,
                protocol_name=type(sender.protocol),
                sender=sender.id.value,
                key=key.value,
                random_id=id.value
            ))
        )
        print(f"http://{self.url}:{self.port}/find_node")

        ret = None
        timeout_error = False
        error = ""
        try:
            print("[Client] Sending find_node RPC...")
            ret = requests.post(
                f"http://{self.url}:{self.port}/find_node",
                data=encoded_data,
                timeout=Constants.REQUEST_TIMEOUT
            )
            print(ret)

        except requests.Timeout as t:
            print("[ERROR] [Client] Timeout error when contacting node.\n", t)
            timeout_error = True
            error = t

        except Exception as e:
            print("[ERROR] [Client]", e)
            # request timed out.
            timeout_error = False
            error = e
        if ret:
            encoded_data = ret.content
            ret_decoded = pickler.decrypt_data(encoded_data)
        else:
            ret_decoded = None
        try:
            if ret_decoded:
                if ret_decoded["contacts"]:
                    contacts = []
                    for val in ret_decoded["contacts"]:
                        new_c = Contact(ID(val["contact"]), val["protocol"])
                        contacts.append(new_c)
                    # Return only contacts with supported protocols.
                    rpc_error = get_rpc_error(id,
                                              ret_decoded,
                                              timeout_error,
                                              ErrorResponse(error_message=str(error), random_id=ID.random_id()))
                    if contacts:
                        ret_contacts = [c for c in contacts if c.protocol is not None]
                        return ret_contacts, rpc_error
            rpc_error = get_rpc_error(id,
                                      ret_decoded,
                                      timeout_error,
                                      ErrorResponse(error_message=str(error), random_id=ID.random_id()))
            return [], rpc_error
        except Exception as e:
            error = RPCError()
            error.protocol_error = True
            print("[Client] Exception thrown: ", e)
            return None, error

    def find_value(self, sender: Contact, key: ID) -> tuple[list[Contact] | None, str | None, RPCError | None]:
        """
        Attempt to find the value in the peer network.

        A null contact list is acceptable as it is a valid return
        if the value is found.
        The caller is responsible for checking the timeoutError flag
        to make sure null contacts is not the result of a timeout
        error.

        :param sender: Sender to find value from
        :param key: Key to check for value from key-value pair
        :return: contacts, value, RPCError
        """
        random_id = ID.random_id()
        encoded_data = encrypt_data(
            dict(FindValueRequest(
                protocol=sender.protocol,
                protocol_name=type(sender.protocol),
                sender=sender.id.value,
                key=key.value,
                random_id=random_id.value
            ))
        )

        ret = None
        try:
            print("[Client] Sending POST")
            ret = requests.post(
                url=f"http://{self.url}:{self.port}/find_value",
                data=encoded_data,
                timeout=Constants.REQUEST_TIMEOUT
            )
            print("[Client] Completed POST")
            timeout_error = False
            error = None

        except requests.Timeout as t:
            print("Timeout error", t)
            timeout_error = True
            error = t

        except Exception as e:
            print("Exception!", e)
            # request timed out.
            timeout_error = False
            error = e

        ret_decoded = None
        if ret:
            encoded_data = ret.content
            ret_decoded = pickler.decrypt_data(encoded_data)

        try:
            contacts = []
            if ret_decoded:
                if ret_decoded["contacts"]:
                    for c in ret_decoded["contacts"]:
                        new_contact = Contact(
                            c["protocol"],  # instantiate_protocol
                            ID(c["contact"])
                        )
                        contacts.append(new_contact)
                        print("about to return")

                return [c for c in contacts if c.protocol is not None], \
                    ret_decoded["value"], \
                    get_rpc_error(
                        random_id, ret_decoded, timeout_error, ErrorResponse(
                            random_id=random_id.value,
                            error_message=str(error))
                    )
            else:
                return [c for c in contacts if c.protocol is not None], "", get_rpc_error(
                        random_id, ret_decoded, timeout_error, ErrorResponse(
                            random_id=random_id.value,
                            error_message=str(error))
                    )
        except Exception as e:
            rpc_error = RPCError(str(e))
            rpc_error.protocol_error = True
            print(f"[Client] Error performing find_value: {rpc_error}")
            return None, None, rpc_error

    def ping(self, sender: Contact) -> RPCError:
        random_id = ID.random_id()
        encoded_data = encrypt_data(
            dict(PingRequest(
                protocol=sender.protocol,
                protocol_name=type(sender.protocol),
                sender=sender.id.value,
                random_id=random_id.value)))

        timeout_error = False
        error = None
        ret = None
        try:
            print("[Client] Sending Ping RPC...")
            ret: requests.Response = requests.post(
                url=f"http://{self.url}:{self.port}/ping",
                data=encoded_data,
                timeout=Constants.REQUEST_TIMEOUT
            )
            print(f"[Client] Received HTTP Response from {ret.url} with code {ret.status_code}")

        except requests.Timeout as t:
            print("[Client] Ping timeout error: ", t)
            timeout_error = True
            error = t

        except Exception as e:
            print("[ERROR] [Client] Other exception thrown (Ping): ", e)
            # request timed out.
            timeout_error = False
            error = e

        ret_base_response = None

        formatted_response = None
        if ret:
            encoded_data = ret.content
            formatted_response = pickler.decrypt_data(encoded_data)

        return get_rpc_error(random_id, formatted_response, timeout_error, ErrorResponse(
            error_message=str(error), random_id=ID.random_id()))

    def store(self,
              sender: Contact,
              key: ID,
              val: str,
              is_cached=False,
              expiration_time_sec=0
              ) -> RPCError:
        random_id = ID.random_id()

        encoded_data = encrypt_data(
            dict(StoreRequest(
                protocol=sender.protocol,
                protocol_name=type(sender.protocol),
                sender=sender.id.value,
                key=key.value,
                value=val,
                is_cached=is_cached,
                expiration_time_sec=expiration_time_sec,
                random_id=random_id.value)))

        timeout_error = False
        error = None
        ret = None

        try:
            print(f"[Client] Running Store POST to http://{self.url}:{self.port}/store")
            ret = requests.post(
                url=f"http://{self.url}:{self.port}/store",
                data=encoded_data,
                timeout=Constants.REQUEST_TIMEOUT
            )
            print("[Client] Store POST done!")

        except requests.Timeout as t:
            print("[Client] Timeout error when contacting node.")
            timeout_error = True
            error = t

        except Exception as e:
            print("Exception!", e)
            # request timed out.
            timeout_error = False
            error = e

        # if ret.status_code == 200:
        # TODO: Add error handling

        formatted_response = None
        if ret:
            encoded_data = ret.content
            formatted_response = pickler.decrypt_data(encoded_data)

        return get_rpc_error(random_id, formatted_response, timeout_error, ErrorResponse(
            error_message=str(error), random_id=ID.random_id()))
