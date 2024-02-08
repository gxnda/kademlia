from abc import abstractmethod
from datetime import datetime

from kademlia.errors import RPCError
from kademlia.id import ID


class IStorage:
    """Interface which 'abstracts the storage mechanism for key-value pairs.''"""

    @abstractmethod
    def contains(self, key: ID) -> bool:
        pass

    @abstractmethod
    def try_get_value(self, key: ID) -> tuple[bool, int | str]:
        pass

    @abstractmethod
    def get(self, key: ID | int) -> str:
        pass

    @abstractmethod
    def get_timestamp(self, key: int) -> datetime:
        pass

    @abstractmethod
    def set(self, key: ID, value: str, expiration_time_sec: int = 0) -> None:
        pass

    @abstractmethod
    def get_expiration_time_sec(self, key: int) -> int:
        pass

    @abstractmethod
    def remove(self, key: int) -> None:
        pass

    @abstractmethod
    def get_keys(self) -> list[int]:
        pass

    @abstractmethod
    def touch(self, key: int) -> None:
        pass


class IProtocol:
    """
    Interface for all protocols to follow.
    """
    def __init__(self):
        self.node = None

    @abstractmethod
    def ping(self, sender) -> RPCError:
        pass

    @abstractmethod
    def find_node(self, sender, key: ID) -> tuple[list, RPCError]:
        pass

    @abstractmethod
    def find_value(self, sender, key: ID) -> tuple[list, str, RPCError]:
        pass

    @abstractmethod
    def store(self, sender, key: ID, val: str, is_cached: bool = False) -> RPCError:
        pass



