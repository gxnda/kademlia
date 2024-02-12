from typing import Callable, TypedDict

from kademlia.contact import Contact
from kademlia.id import ID


class FindResult(TypedDict):
    """
    Has elements: contacts, val, found, found_by
    """
    contacts: list[Contact]
    val: str | None
    found: bool
    found_by: Contact | None


class ContactQueueItem(TypedDict):
    key: ID
    contact: Contact
    rpc_call: Callable
    closer_contacts: list[Contact]
    further_contacts: list[Contact]
    find_result: FindResult


class GetCloserNodesReturn(TypedDict):
    found: bool
    found_by: Contact | None
    val: str | None


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
    Interface used for TCP Subnetting.
    """
    subnet: int


class FindNodeSubnetRequest(FindNodeRequest, ITCPSubnet, TypedDict):
    pass


class FindValueSubnetRequest(FindValueRequest, ITCPSubnet, TypedDict):
    pass


class PingSubnetRequest(PingRequest, ITCPSubnet, TypedDict):
    pass


class StoreSubnetRequest(StoreRequest, ITCPSubnet, TypedDict):
    pass


class CommonRequest(TypedDict):
    """
    This includes all possible headers that could be passed.
    """
    protocol: any  # IProtocol
    protocol_name: str
    random_id: int
    sender: int
    key: int
    value: str | None
    is_cached: bool
    expiration_time_sec: int


class BaseResponse(TypedDict):
    """
    Has element random_id (int).
    """
    random_id: int


class ErrorResponse(BaseResponse, TypedDict):
    error_message: str


class ContactResponse(TypedDict):
    contact: int
    protocol: dict  # Or object?
    protocol_name: dict


class FindNodeResponse(BaseResponse, TypedDict):
    contacts: list[ContactResponse]


class FindValueResponse(TypedDict, BaseResponse):
    contacts: list[ContactResponse]
    value: str


class PingResponse(TypedDict, BaseResponse):
    pass


class StoreResponse(BaseResponse):
    pass


class StoreValue(TypedDict):
    """
    Has attributes:

    value: str

    republish_timestamp: datetime

    expiration_time: int
    """
    value: str  # | bytes
    republish_timestamp: str
    expiration_time: int
