import asyncio
import json
import logging
import sys
from typing import Any

import aiohttp

from . import pickler
from .constants import Constants
from .contact import Contact
from .dictionaries import (ErrorResponse, FindNodeSubnetRequest,
                                       FindValueSubnetRequest, PingSubnetRequest, StoreSubnetRequest, FindNodeRequest,
                                       FindValueRequest, PingRequest, StoreRequest)
from .errors import RPCError
from .id import ID
from .interfaces import IProtocol
from .node import Node
from .pickler import encode_data


logger = logging.getLogger("__main__")


def get_rpc_error(id: ID,
                  ret: dict | None,
                  timeout_error: bool,
                  peer_error: ErrorResponse) -> RPCError:
    error = RPCError()
    if ret:
        error.id_mismatch_error = id != ret.get("random_id")
    else:
        error.id_mismatch_error = False
    error.timeout_error = timeout_error
    # logger.info(f"get_rpc_error: {peer_error["error_message"]},
    # {type(peer_error['error_message'])}")
    error.peer_error = peer_error["error_message"] not in ["", None, "None"]
    if peer_error["error_message"]:
        error.peer_error_message = peer_error["error_message"]

    return error


def decode_protocol(protocol: dict) -> IProtocol:
    if not protocol:
        raise Exception("Protocol cannot be None.")
    elif not protocol.get("type"):
        raise Exception("Protocol type cannot be None.")

    if protocol["type"] == "TCPProtocol":
        return TCPProtocol(protocol["url"], protocol["port"])
    elif protocol["type"] == "TCPSubnetProtocol":
        return TCPSubnetProtocol(protocol["url"], protocol["port"], protocol["subnet"])
    else:
        logger.debug(f"Unknown protocol: {protocol}")
        raise Exception(f"Unknown protocol type: {protocol['type']}")


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

    def encode(self):
        raise Exception("VirtualProtocol should not be encoded (only for testing, not for use across HTTP).")

    def ping(self, sender: Contact) -> RPCError:
        """
        Pings sender if we respond.

        :param sender:
        :return:
        """
        if self.responds:
            self.node.ping(sender)
            return RPCError.no_error()
        else:
            error = RPCError(
                "Time out while pinging contact - VirtualProtocol does not respond.",
                timeout_error=not self.responds
            )
            return error

    def find_node(self, sender: Contact,
                  key: ID) -> tuple[list[Contact], RPCError]:
        """
        Finds K close contacts to a given ID, while excluding the sender.
        It also adds the sender if it hasn't seen it before.
        :param key: K close contacts are found near this ID.
        :param sender: Contact to be excluded and added if new.
        :return: list of K (or less) contacts near the key, and an error that may need to be handled.
        """
        return self.node.find_node(sender=sender, key=key)[0], RPCError.no_error()

    def find_value(self, sender: Contact,
                   key: ID) -> tuple[list[Contact] | None, str | None, RPCError]:
        """
        Sends key values if new contact, then attempts to find the value of a key-value pair in
        our storage (then cache storage), given the key. If it cannot do that, it will return
        K contacts that are closer to the key than it is.
        """
        contacts, val = self.node.find_value(sender=sender, key=key)
        return contacts, val, RPCError.no_error()

    def store(self,
              sender: Contact,
              key: ID,
              val: str,
              is_cached=False,
              exp_time_sec: int = 0) -> RPCError:
        """
        Stores the key-value on the remote peer.
        """
        self.node.store(sender=sender,
                        key=key,
                        val=val,
                        is_cached=is_cached,
                        expiration_time_sec=exp_time_sec)

        return RPCError.no_error()


class TCPSubnetProtocol(IProtocol):

    def __init__(self, url: str, port: int, subnet: int):
        self.url = url
        self.port = port
        self.responds = True
        self.subnet = subnet
        self.type = "TCPSubnetProtocol"

    def __repr__(self):
        return f"{self.type}({self.url}:{self.port}, subnet={self.subnet})"

    def encode(self) -> dict[str, Any]:
        return {
            "type": self.type,
            "url": self.url,
            "port": self.port,
            "subnet": self.subnet
        }

    async def _make_rpc_request(self, method: str, data_to_send):
        error = None
        timeout_error = False
        formatted_response = None
        async with aiohttp.ClientSession() as session:
            try:
                logger.info(f"[Client] Sending {method} RPC...")
                async with session.post(
                        url=f"http://{self.url}:{self.port}/{method}",
                        data=data_to_send,
                        headers={'Content-Type': 'application/json'},
                        timeout=Constants.REQUEST_TIMEOUT_SEC
                ) as ret:
                    logger.info(f"[Client] Received {method} response from"
                                f" {ret.url} with code {ret.status}")

                    if ret.ok:
                        try:
                            formatted_response = await ret.json(
                                loads=pickler.decode_data)
                        except json.JSONDecodeError as e:
                            error = f"JSON decode failed: {e}"
                            logger.error(f"[Client] Invalid JSON response: "
                                         f"{await ret.text()}")
                    else:
                        error = f"HTTP Error {ret.status}"
                        logger.error(f"[Client] Server error: {error}")

            except asyncio.TimeoutError:
                logger.error("[Client] Ping timeout error")
                timeout_error = True
                error = "Request timed out"
            except aiohttp.ClientError as e:
                logger.error(f"[Client] Network error: {e}")
                error = str(e)
            except Exception as e:
                logger.error(f"[Client] Unexpected error: {e}")
                raise e

        return formatted_response, error, timeout_error

    async def find_node(self, sender: Contact, key: ID) -> (
            tuple)[list[Contact] | None, RPCError]:
        """
        Encodes all of the data that is needed into a FindNodeSubnetRequest,
        Which is then pickled and posted using the ‘requests’ library to self.url and
        self.port using a “/find_node” endpoint. This makes sense because our node doesn’t
        call this – other nodes will call this method to contact us. This handles a timeout error
        by creating a timeout error RPCError, any other errors are also turned into and RPCError by
        get_rpc_error().

        :param sender:
        :param key:
        :return:
        """
        random_id: ID = ID.random_id()
        encoded_data = encode_data(
            dict(FindNodeSubnetRequest(
                protocol=sender.protocol.encode(),
                subnet=self.subnet,
                sender=sender.id.value,
                key=key.value,
                random_id=random_id.value
            ))
        )

        decoded_response, error, timeout_error = await self._make_rpc_request(
            "find_node", encoded_data)

        try:
            if decoded_response["contacts"]:
                contacts = []
                for val in decoded_response["contacts"]:
                    new_c = Contact(
                        ID(val["contact"]),
                        decode_protocol(val["protocol"])
                    )
                    contacts.append(new_c)
                # Return only contacts with supported protocols.
                rpc_error = get_rpc_error(random_id,
                                          decoded_response,
                                          timeout_error,
                                          ErrorResponse(
                                              error_message=str(
                                                  error),
                                              random_id=ID.random_id()))
                if contacts:
                    ret_contacts = [c for c in contacts
                                    if
                                    c.protocol is not None]
                    return ret_contacts, rpc_error
                else:
                    return [], rpc_error

        except Exception as e:
            error = RPCError()
            error.protocol_error = True
            logger.error(f"[Client] Exception thrown: {e}")
            return None, error

        return [], RPCError.no_error()

    async def find_value(self, sender: Contact, key: ID) -> (
            tuple)[list[Contact] | None, str | None, RPCError | None]:
        """
        Attempt to find the value in the peer network.

        A null contact list is acceptable as it is a valid return
        if the value is found.
        The caller is responsible for checking the timeoutError flag
        to make sure null contacts is not the result of a timeout
        error.

        Encodes all the data that is needed into a FindValueSubnetRequest,
        Which is then pickled and posted using the ‘requests’ library to self.url
        and self.port using a “/find_value” endpoint. This makes sense because our
        node doesn’t call this – other nodes will call this method to contact us.
        This handles a timeout error by creating a timeout error RPCError, any other
        errors are also turned into and RPCError by get_rpc_error().


        :param sender: Sender to find value from
        :param key: Key to check for value from key-value pair
        :return: contacts, value, RPCError
        """
        random_id = ID.random_id()
        encoded_data = encode_data(
            dict(FindValueSubnetRequest(
                protocol=sender.protocol.encode(),
                subnet=self.subnet,
                sender=sender.id.value,
                key=key.value,
                random_id=random_id.value
            ))
        )

        ret_decoded, error, timeout_error = await self._make_rpc_request(
            "find_value", encoded_data)

        try:
            contacts = []
            if ret_decoded:
                if ret_decoded["contacts"]:
                    for c in ret_decoded["contacts"]:
                        new_contact = Contact(
                            ID(c["contact"]),
                            decode_protocol(c["protocol"]),
                        )
                        contacts.append(new_contact)

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
            logger.error(f"[Client] Error performing find_value: {rpc_error}")
            return None, None, rpc_error

    async def ping(self, sender: Contact) -> RPCError:
        """
        Encodes all of the data that is needed into a PingSubnetRequest,
        Which is then pickled and posted using the ‘requests’ library to self.url and
        self.port using a “/ping” endpoint. This makes sense because our node doesn’t call
        this – other nodes will call this method to contact us. This handles a timeout error
        by setting a timeout_error flag, which is passed into get_rpc_error at the end of
        the method. Any other exceptions are handled in a similar fashion.

        The response is then decoded if there is one, then an RPCError is returned.

        :param sender:
        :return:
        """
        random_id = ID.random_id()
        encoded_data = encode_data(
            dict(PingSubnetRequest(
                protocol=sender.protocol.encode(),
                subnet=self.subnet,
                sender=sender.id.value,
                random_id=random_id.value)))

        formatted_response, error, timeout_error = await self._make_rpc_request(
            "ping", encoded_data)

        return get_rpc_error(random_id, formatted_response, timeout_error,
                             ErrorResponse(
                                 error_message=str(error),
                                 random_id=ID.random_id()))

    async def store(self,
              sender: Contact,
              key: ID,
              val: str,
              is_cached=False,
              expiration_time_sec=0
              ) -> RPCError:
        random_id = ID.random_id()

        logger.info("store val type" + str(type(val)) +
                    f", store val size: {sys.getsizeof(val)}")
        encoded_data = encode_data(
            dict(StoreSubnetRequest(
                protocol=sender.protocol.encode(),
                subnet=self.subnet,
                sender=sender.id.value,
                key=key.value,
                value=val,
                is_cached=is_cached,
                expiration_time_sec=expiration_time_sec,
                random_id=random_id.value)))
        logger.info(f"StoreSubnetRequest size: {sys.getsizeof(encoded_data)} "
                    f"bytes, value size: "
                    f"{sys.getsizeof(encode_data({"val": val}))}, "
                    f"protocol size: "
                    f"{sys.getsizeof(sender.protocol.encode())},")

        formatted_response, error, timeout_error = await (
            self._make_rpc_request("store", encoded_data))

        # logger.info(f"Error info in store: {error}")
        return get_rpc_error(random_id, formatted_response, timeout_error, ErrorResponse(
            error_message=str(error), random_id=ID.random_id()))



class TCPProtocol(IProtocol):

    def __init__(self, url: str, port: int):
        self.url = url
        self.port = port
        self.responds = True
        self.type = "TCPSubnetProtocol"

    def __repr__(self):
        return f"{self.type}({self.url}:{self.port})"

    def encode(self) -> dict[str, Any]:
        return {
            "type": self.type,
            "url": self.url,
            "port": self.port,
        }

    async def _make_rpc_request(self, method: str, data_to_send):
        error = None
        timeout_error = False
        formatted_response = None
        async with aiohttp.ClientSession() as session:
            try:
                logger.info(f"[Client] Sending {method} RPC...")
                async with session.post(
                        url=f"http://{self.url}:{self.port}/{method}",
                        data=data_to_send,
                        headers={'Content-Type': 'application/json'},
                        timeout=Constants.REQUEST_TIMEOUT_SEC
                ) as ret:
                    logger.info(f"[Client] Received {method} response from"
                                f" {ret.url} with code {ret.status}")

                    if ret.ok:
                        try:
                            formatted_response = await ret.json(
                                loads=pickler.decode_data)
                        except json.JSONDecodeError as e:
                            error = f"JSON decode failed: {e}"
                            logger.error(f"[Client] Invalid JSON response: "
                                         f"{await ret.text()}")
                    else:
                        error = f"HTTP Error {ret.status}"
                        logger.error(f"[Client] Server error: {error}")

            except asyncio.TimeoutError:
                logger.error("[Client] Ping timeout error")
                timeout_error = True
                error = "Request timed out"
            except aiohttp.ClientError as e:
                logger.error(f"[Client] Network error: {e}")
                error = str(e)
            except Exception as e:
                logger.error(f"[Client] Unexpected error: {e}")
                raise e

        return formatted_response, error, timeout_error

    async def find_node(self, sender: Contact, key: ID) -> (
            tuple)[list[Contact] | None, RPCError]:
        """
        Encodes all of the data that is needed into a FindNodeSubnetRequest,
        Which is then pickled and posted using the ‘requests’ library to self.url and
        self.port using a “/find_node” endpoint. This makes sense because our node doesn’t
        call this – other nodes will call this method to contact us. This handles a timeout error
        by creating a timeout error RPCError, any other errors are also turned into and RPCError by
        get_rpc_error().

        :param sender:
        :param key:
        :return:
        """
        random_id: ID = ID.random_id()
        encoded_data = encode_data(
            dict(FindNodeRequest(
                protocol=sender.protocol.encode(),
                sender=sender.id.value,
                key=key.value,
                random_id=random_id.value
            ))
        )

        decoded_response, error, timeout_error = await self._make_rpc_request(
            "find_node", encoded_data)

        try:
            if decoded_response["contacts"]:
                contacts = []
                for val in decoded_response["contacts"]:
                    new_c = Contact(
                        ID(val["contact"]),
                        decode_protocol(val["protocol"])
                    )
                    contacts.append(new_c)
                # Return only contacts with supported protocols.
                rpc_error = get_rpc_error(random_id,
                                          decoded_response,
                                          timeout_error,
                                          ErrorResponse(
                                              error_message=str(
                                                  error),
                                              random_id=ID.random_id()))
                if contacts:
                    ret_contacts = [c for c in contacts
                                    if
                                    c.protocol is not None]
                    return ret_contacts, rpc_error
                else:
                    return [], rpc_error

        except Exception as e:
            error = RPCError()
            error.protocol_error = True
            logger.error(f"[Client] Exception thrown: {e}")
            return None, error

        return [], RPCError.no_error()

    async def find_value(self, sender: Contact, key: ID) -> (
            tuple)[list[Contact] | None, str | None, RPCError | None]:
        """
        Attempt to find the value in the peer network.

        A null contact list is acceptable as it is a valid return
        if the value is found.
        The caller is responsible for checking the timeoutError flag
        to make sure null contacts is not the result of a timeout
        error.

        Encodes all the data that is needed into a FindValueSubnetRequest,
        Which is then pickled and posted using the ‘requests’ library to self.url
        and self.port using a “/find_value” endpoint. This makes sense because our
        node doesn’t call this – other nodes will call this method to contact us.
        This handles a timeout error by creating a timeout error RPCError, any other
        errors are also turned into and RPCError by get_rpc_error().


        :param sender: Sender to find value from
        :param key: Key to check for value from key-value pair
        :return: contacts, value, RPCError
        """
        random_id = ID.random_id()
        encoded_data = encode_data(
            dict(FindValueRequest(
                protocol=sender.protocol.encode(),
                sender=sender.id.value,
                key=key.value,
                random_id=random_id.value
            ))
        )

        ret_decoded, error, timeout_error = await self._make_rpc_request(
            "find_value", encoded_data)

        try:
            contacts = []
            if ret_decoded:
                if ret_decoded["contacts"]:
                    for c in ret_decoded["contacts"]:
                        new_contact = Contact(
                            ID(c["contact"]),
                            decode_protocol(c["protocol"]),
                        )
                        contacts.append(new_contact)

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
            logger.error(f"[Client] Error performing find_value: {rpc_error}")
            return None, None, rpc_error

    async def ping(self, sender: Contact) -> RPCError:
        """
        Encodes all of the data that is needed into a PingSubnetRequest,
        Which is then pickled and posted using the ‘requests’ library to self.url and
        self.port using a “/ping” endpoint. This makes sense because our node doesn’t call
        this – other nodes will call this method to contact us. This handles a timeout error
        by setting a timeout_error flag, which is passed into get_rpc_error at the end of
        the method. Any other exceptions are handled in a similar fashion.

        The response is then decoded if there is one, then an RPCError is returned.

        :param sender:
        :return:
        """
        random_id = ID.random_id()
        encoded_data = encode_data(
            dict(PingRequest(
                protocol=sender.protocol.encode(),
                sender=sender.id.value,
                random_id=random_id.value)))

        formatted_response, error, timeout_error = await self._make_rpc_request(
            "ping", encoded_data)

        return get_rpc_error(random_id, formatted_response, timeout_error,
                             ErrorResponse(
                                 error_message=str(error),
                                 random_id=ID.random_id()))

    async def store(self,
                    sender: Contact,
                    key: ID,
                    val: str,
                    is_cached=False,
                    expiration_time_sec=0
                    ) -> RPCError:
        random_id = ID.random_id()

        logger.info("store val type" + str(type(val)) +
                    f", store val size: {sys.getsizeof(val)}")
        encoded_data = encode_data(
            dict(StoreRequest(
                protocol=sender.protocol.encode(),
                sender=sender.id.value,
                key=key.value,
                value=val,
                is_cached=is_cached,
                expiration_time_sec=expiration_time_sec,
                random_id=random_id.value)))
        logger.info(f"StoreSubnetRequest size: {sys.getsizeof(encoded_data)} "
                    f"bytes, value size: "
                    f"{sys.getsizeof(encode_data({"val": val}))}, "
                    f"protocol size: "
                    f"{sys.getsizeof(sender.protocol.encode())},")

        formatted_response, error, timeout_error = await (
            self._make_rpc_request("store", encoded_data))

        # logger.info(f"Error info in store: {error}")
        return get_rpc_error(random_id, formatted_response, timeout_error, ErrorResponse(
            error_message=str(error), random_id=ID.random_id()))
