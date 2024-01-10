from dataclasses import dataclass

from kademlia import ID
from typing import TypedDict


@dataclass
class BaseRequest:
    protocol: object
    protocol_name: str
    sender: int
    random_id: int = ID.random_id().value


@dataclass
class FindNodeRequest(BaseRequest):
    key: int


@dataclass
class FindValueRequest(BaseRequest):
    key: int


@dataclass
class PingRequest(BaseRequest):
    pass


@dataclass
class StoreRequest(BaseRequest):
    key: int
    value: str
    is_cached: bool
    expiration_time_sec: int


@dataclass
class ITCPSubnet:
    """
    Interface used for TCP.
    """
    subnet: int


@dataclass
class FindNodeSubnetRequest(FindNodeRequest, ITCPSubnet):
    subnet: int


@dataclass
class FindValueSubnetRequest(FindValueRequest, ITCPSubnet):
    subnet: int


@dataclass
class PingSubnetRequest(PingRequest, ITCPSubnet):
    subnet: int


@dataclass
class StoreSubnetRequest(StoreRequest, ITCPSubnet):
    subnet: int


@dataclass
class CommonRequest:
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
