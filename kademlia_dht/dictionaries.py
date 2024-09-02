from typing import Callable, TypedDict

from kademlia_dht.contact import Contact
from kademlia_dht.id import ID


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
    protocol: dict
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
    protocol: dict  # IProtocol
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


class TorrentFile(TypedDict):
    """
        length — size of the file in bytes.

        path — a list of strings corresponding to subdirectory names, the last of which is the actual file name

        length — size of the file in bytes (only when one file is being shared though)

        name — suggested filename where the file is to be saved (if one file)/suggested directory name where
        the files are to be saved (if multiple files)

        piece length — number of bytes per piece. This is commonly 28 KiB = 256 KiB = 262,144 B.

        pieces — a hash list, i.e., a concatenation of each piece's SHA-1 hash. As SHA-1 returns a 160-bit hash,
        pieces will be a string whose length is a multiple of 20 bytes. If the torrent contains multiple files,
        the pieces are formed by concatenating the files in the order they appear in the files dictionary
        (i.e., all pieces in the torrent are the full piece length except for the last piece, which may be shorter).
    """
    length: int
    path: list[str]
    name: str
    piece_length: int
    pieces: list[int]



class TorrentDict:
    """
    A torrent file contains a list of files and integrity metadata about all the pieces,
    and optionally contains a large list of trackers.

    A torrent file is a bencoded dictionary with the following keys (the keys in any bencoded dictionary
    are lexicographically ordered):

    announce — the URL of the high tracker

    info — this maps to a dictionary whose keys are very dependent on whether one or more files are being shared:

    files — a list of dictionaries each corresponding to a file (only when multiple files are being shared).    
    """

    announce: str
    info: dict
    files: list[TorrentFile]