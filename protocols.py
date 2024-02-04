from contact import Contact
from dictionaries import BaseResponse, ErrorResponse, FindNodeSubnetRequest, FindValueSubnetRequest, PingSubnetRequest, \
    StoreSubnetRequest
from errors import RPCError
from id import ID
from interfaces import IProtocol
from node import Node
from pickler import encode_data


def get_rpc_error(id: ID,
                  resp: BaseResponse,
                  timeout_error: bool,
                  peer_error: ErrorResponse) -> RPCError:
    error = RPCError()
    error.id_mismatch_error = id != resp["random_id"]
    error.timeout_error = timeout_error
    error.peer_error = peer_error is not None
    if peer_error:
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
        self.subnet = None
        self.type = "TCPSubnetProtocol"

    def find_node(self, sender: Contact, key: ID) -> tuple[list[Contact] | None, RPCError]:
        id: ID = ID.random_id()
        encoded_data = encode_data(
            dict(FindNodeSubnetRequest(
                protocol=sender.protocol,
                protocol_name=type(sender.protocol),
                subnet=self.subnet,
                sender=sender.id.value,
                key=key.value,
                random_id=id.value
            ))
        )
        ret, error, timeout_error = requests.post(
            f"http://{self.url}:{self.port}/find_node",
            data=encoded_data
        )
        try:
            contacts = []
            if ret:
                if ret["contacts"]:  # TODO: Is ret a dictionary?
                    contacts = []
                    for val in ret["contacts"]:
                        new_c = Contact(val["protocol"], val[""])
                        contacts.append(new_c)
                    # Return only contacts with supported protocols.
                    if contacts:
                        return [c for c in contacts if c.protocol is not None], get_rpc_error(id, ret, timeout_error,
                                                                                              error)
        except Exception as e:
            error = RPCError()
            error.protocol_error = True
            return None, error

    def find_value(self, sender: Contact, key: ID) -> tuple[list[Contact] | None, str | None, RPCError]:
        """
        Attempt to find the value in the peer network.

        A null contact list is acceptable as it is a valid return
        if the value is found.
        The caller is responsible for checking the timeoutError flag
        to make sure null contacts is not the result of a timeout
        error.
        """
        random_id = ID.random_id()
        encoded_data = encode_data(
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
            ret = requests.post(
                url=f"http://{self.url}:{self.port}/find_value",
                data=encoded_data
            )
            timeout_error = False
            error = None
        except TimeoutError as e:
            # request timed out.
            timeout_error = True
            error = e
            print("TimeoutError:", e)

        try:
            contacts = []
            if ret:
                if ret["contacts"]:
                    for c in ret["contacts"]:
                        new_contact = Contact(
                            c["protocol"],  # instantiate_protocol
                            ID(c["contact"])
                        )
                        contacts.append(new_contact)
                        return [c for c in contacts if c.protocol is not None], \
                            ret["value"], \
                            get_rpc_error(
                                random_id, ret, timeout_error, ErrorResponse(
                                    random_id=random_id.value,
                                    error_message=str(error))
                            )
        except Exception as e:
            rpc_error = RPCError(str(e))
            rpc_error.protocol_error = True
            print(f"Error performing find_value: {rpc_error}")
            return None, None, rpc_error

    def ping(self, sender: Contact) -> RPCError:
        random_id = ID.random_id()
        encoded_data = encode_data(
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
            ret = requests.post(
                url=f"http://{self.url}:{self.port}/ping",
                data=encoded_data
            )

        except TimeoutError as e:
            # request timed out.
            timeout_error = True
            error = e

        return get_rpc_error(random_id, ret, timeout_error, ErrorResponse(
            error_message=str(error), random_id=ID.random_id()))

    def store(self,
              sender: Contact,
              key: ID,
              val: str,
              is_cached=False,
              expiration_time_sec=0
              ) -> RPCError:

        random_id = ID.random_id()
        encoded_data = encode_data(
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
            ret = requests.post(
                url=f"http://{self.url}:{self.port}/store",
                data=encoded_data
            )
        except TimeoutError as e:
            # request timed out.
            timeout_error = True
            error = e

        return get_rpc_error(random_id, ret, timeout_error, ErrorResponse(
            error_message=str(error), random_id=ID.random_id()))
