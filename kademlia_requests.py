from dataclasses import dataclass

from kademlia import ID
from typing import TypedDict


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
    subnet: int


class FindValueSubnetRequest(FindValueRequest, ITCPSubnet, TypedDict):
    subnet: int


class PingSubnetRequest(PingRequest, ITCPSubnet, TypedDict):
    subnet: int


class StoreSubnetRequest(StoreRequest, ITCPSubnet, TypedDict):
    subnet: int


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
