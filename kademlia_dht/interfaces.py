from abc import abstractmethod
from datetime import datetime
from typing import Any

from kademlia_dht.errors import RPCError
from kademlia_dht.id import ID


class IStorage:
    """Interface which 'abstracts the storage mechanism for key-value pairs.''"""
    @abstractmethod
    def contains(self, key: ID) -> bool:
        """
        Returns if the given key is contained in the storage object.
        :param key:
        :return:
        """
        pass

    @abstractmethod
    def try_get_value(self, key: ID) -> tuple[bool, str]:
        """
        Attempts to get a value from a key-value pair, returns ‘False, “”’ if it is not there; returns
        ‘True, value’ if it is there.
        :param key:
        :return:
        """
        pass

    @abstractmethod
    def get(self, key: ID | int) -> str:
        """
        Tries to return value from key-value pair, given key.
        :param key:
        :return: StoreValue
        """
        pass

    @abstractmethod
    def get_timestamp(self, key: int) -> datetime:
        """
        Returns timestamp of key-value pair, given key.
        :param key:
        :return:
        """
        pass

    @abstractmethod
    def set(self, key: ID, value: str, expiration_time_sec: int = 0) -> None:
        """
        Sets key-value pair with expiration time.
        :param key:
        :param value:
        :param expiration_time_sec:
        :return:
        """
        pass

    @abstractmethod
    def get_expiration_time_sec(self, key: int) -> int:
        """
        Gets expiration time from key-value pair, given key.
        :param key:
        :return:
        """
        pass

    @abstractmethod
    def remove(self, key: int) -> None:
        """
        Removes key-value pair from the storage object, given key.
        :param key:
        :return:
        """
        pass

    @abstractmethod
    def get_keys(self) -> list[int]:
        """
        Returns all keys from key-value pairs stored in the storage object.
        :return:
        """
        pass

    @abstractmethod
    def touch(self, key: int) -> None:
        """
        Sets timestamp of key-value to current time, given key.
        :param key:
        :return:
        """
        pass


class IProtocol:
    """
    Interface for all protocols to follow.
    """

    @abstractmethod
    def encode(self) -> dict[str, Any]:
        pass

    @abstractmethod
    def ping(self, sender) -> RPCError:
        """
        Handles an incoming ping request from “sender”, returns an RPCError object.
        :param sender:
        :return:
        """
        pass

    @abstractmethod
    def find_node(self, sender, key: ID) -> tuple[list, RPCError]:
        """
        Attempts to find K close nodes to key, returning them and an RPCError object.
        :param sender:
        :param key:
        :return:
        """
        pass

    @abstractmethod
    def find_value(self, sender, key: ID) -> tuple[list, str, RPCError]:
        """
        Attempts to find value from key-value pair, if it cannot be found, a list of K
        closer contacts are returned. An RPCError object is returned as well to indicate
        any errors that have occurred.

        :param sender:
        :param key:
        :return:
        """
        pass

    @abstractmethod
    def store(self, sender, key: ID, val: str, is_cached: bool = False, exp_time_sec: int = 0) -> RPCError:
        """
        Attempts to save a key-value pair to storage, it caches it instead of
        storing if is_stored, and it expires after exp_time_sec.

        :param sender:
        :param key:
        :param val:
        :param is_cached:
        :param exp_time_sec:
        :return:
        """
        pass
