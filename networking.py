import threading
from typing import TypedDict
from http.server import BaseHTTPRequestHandler, HTTPServer
import json
import pickle


class UnknownRequestError(Exception):
    pass


class BaseRequest(TypedDict):
    protocol: object
    protocol_name: str
    sender: int
    random_id: int


class FindNodeRequest(BaseRequest, TypedDict):
    key: int


class FindValueRequest(BaseRequest, TypedDict):
    key: int


class PingRequest(BaseRequest, TypedDict):
    pass


class StoreRequest(BaseRequest, TypedDict):
    key: int
    value: str
    is_cached: bool
    expiration_time_sec: int


class ITCPSubnet(TypedDict):
    """
    Interface used for TCP.
    """
    subnet: int


class FindNodeSubnetRequest(FindNodeRequest, ITCPSubnet, TypedDict):
    # subnet: int
    pass


class FindValueSubnetRequest(FindValueRequest, ITCPSubnet, TypedDict):
    # subnet: int
    pass


class PingSubnetRequest(PingRequest, ITCPSubnet, TypedDict):
    # subnet: int
    pass


class StoreSubnetRequest(StoreRequest, ITCPSubnet, TypedDict):
    # subnet: int
    pass


class CommonRequest(TypedDict):
    """
    This includes all possible headers that could be passed.
    """
    protocol: object
    protocol_name: str
    random_id: int
    sender: int
    key: int
    value: int
    is_cached: bool
    expiration_time_sec: int


class BaseResponse(TypedDict):
    random_id: int


class ErrorResponse(TypedDict, BaseResponse):
    error_message: str


class ContactResponse(TypedDict):
    contact: int
    protocol: dict  # Or object?
    protocol_name: dict


class FindNodeResponse(TypedDict, BaseResponse):
    contacts: list[ContactResponse]


class FindValueResponse(TypedDict, BaseResponse):
    contacts: list[ContactResponse]
    value: str


class PingResponse(TypedDict, BaseResponse):
    pass


class StoreResponse(BaseResponse):
    pass


class TCPServer(HTTPServer):  # TODO: Create.
    def __init__(self, node):
        server_address: tuple[str, int] = (node.our_contact.protocol.ip, node.our_contact.protocol.port)
        super().__init__(
            server_address=server_address,
            RequestHandlerClass=BaseHTTPRequestHandler
        )
        self.route_packets = {
            "//Ping": PingRequest,
            "//Store": StoreRequest,
            "//FindNode": FindNodeRequest,
            "//FindValue": FindValueRequest
        }
        self.node = node


class TCPSubnetServer(HTTPServer):
    def __init__(self, server_address: tuple[str, int]):
        super().__init__(
            server_address=server_address,
            RequestHandlerClass=BaseHTTPRequestHandler
        )

        # TODO: Should these be double slashed?
        self.route_packets: dict[str: type] = {
            "//ping": PingRequest,  # "ping" should refer to type PingRequest
            "//store": StoreRequest,  # "store" should refer to type StoreRequest
            "//find_node": FindNodeRequest,  # "find_node" should refer to type FindNodeRequest
            "//find_value": FindValueRequest  # "find_value" should refer to type FindValueRequest
        }

        self.subnets: dict = {}

    def register_protocol(self, subnet: int, node):
        self.subnets[subnet] = node

    def process_request(self, context: BaseHTTPRequestHandler):
        """
        I don't know much about HTTP Servers.
        "The server is a straightforward HttpListener implemented as a C# HttpListenerContext
        object, but note how the subnet ID is used to route the request to the specific node
        associated with the subnet." - Marc Clifton

        I am planning to take in a BaseHTTPRequestHandler object and read the body.
        The body should be a JSON containing key value pairs of the following values:

            protocol: object
            protocol_name: str
            random_id: int
            sender: int
            key: int
            value: int
            is_cached: bool
            expiration_time_sec: int
            subnet: int
        """
        context.handle_one_request()

        # TODO: Add Encryption !!!!!!!

        encoded_request: bytes = context.rfile.read()
        decoded_request: str = encoded_request.decode("utf-8")
        request = decoded_request

        key_value_pickled_pairs: dict = json.loads(request)
        request_dict: dict = {}
        for key in key_value_pickled_pairs:
            # All objects are sent using HTTP as pickle objects to keep protocol objects intact.
            request_dict[key] = pickle.loads(key_value_pickled_pairs[key])

        print(context.request, context.command)
        if context.command == "POST":
            path: str = context.path
            # Remove "//"
            # Prefix our call with "Server" so that the method name is unambiguous.
            method_name: str = "Server" + path[2:]  # path.substring(2)

            # What type is the request?
            try:
                # path is something like //ping or //find_node
                request_type: type | None = self.route_packets[path]
            except KeyError:
                request_type: type | None = None

            # if we know what the request wants (if it's a ping/find_node RPC etc.)
            if request_type:
                subnet: int = request_dict["subnet"]
                common_request: CommonRequest = CommonRequest(
                    protocol=request_dict.get("protocol"),
                    protocol_name=request_dict.get("protocol_name"),
                    random_id=request_dict.get("random_id"),
                    sender=request_dict.get("sender"),
                    key=request_dict.get("key"),
                    value=request_dict.get("value"),
                    is_cached=request_dict.get("is_cached"),
                    expiration_time_sec=request_dict.get("expiration_time_sec")
                )

                # If we know the node on the subnet, this should always happen right?
                # Because this is for testing on the same PC.
                node = self.subnets.get(subnet)
                if node:
                    # TODO: Make Asynchronous
                    new_thread = threading.Thread(
                        target=CommonRequestHandler,  # TODO: This function does not exist.
                        args=(method_name, common_request, node, context)
                    )
                    new_thread.start()

                else:
                    send_error_response(
                        context,
                        ErrorResponse("Subnet node not found.")
                    )

                # context.close_connection = True
