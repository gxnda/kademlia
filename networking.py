from kademlia import ID
from typing import TypedDict
from http.server import BaseHTTPRequestHandler, HTTPServer
import json


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
    For passing to Node handlers with common parameters.
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


def process_request(context: BaseHTTPRequestHandler):
    data: str = context.request
    print(context.request, context.command)
    if context.command == "POST":
        request_type: type
        path: str = context.path
        # Remove "//"
        # Prefix our call with "server" so that the method name is unambiguous.
        method_name: str = "Server" + path[2:]  # path.substring(2)
        if route_packets.try_get_value(path, request_type):
            commonrequest: CommonRequest = json.load(data)
            subnet: int = json.load(data, request_type)["subnet"]

            node: INode
            if subnets.try_get_value(subnet, node):
                # await Task.run()
                Task.run(lambda: CommonRequestHandler(
                    method_name,
                    commonrequest,
                    node,
                    context
                    )
                )
            else:
                send_error_response(
                    context,
                    ErrorResponse("Subnet node not found.")
                )

            context.response.close()
