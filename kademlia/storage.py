from datetime import datetime

from kademlia.id import ID
from kademlia.interfaces import IStorage


class VirtualStorage(IStorage):
    """
    Simple storage mechanism that stores things in memory.
    """

    def __init__(self):
        self._store: dict[int, str] = {}

    def contains(self, key: ID) -> bool:
        """
        Returns a boolean stating whether a key is storing something.
        """
        return key.value in list(self._store.keys())

    def get(self, key: ID | int) -> str:
        """
        Returns stored value, associated with given key value.
        :param key: Type ID or Integer, key value to be searched.
        :return:
        """
        if isinstance(type(key), ID):
            return self._store[key.value]
        elif isinstance(type(key), int):
            return self._store[key]
        else:
            raise TypeError("'get()' parameter 'key' must be type ID or int.")

    def get_timestamp(self, key: int) -> datetime:
        pass

    def set(self, key: ID, value: str, expiration_time_sec: int = 0) -> None:
        self._store[key.value] = value
        # TODO: Add expiration time to this.

    def get_expiration_time_sec(self, key: int) -> int:
        pass

    def remove(self, key: int) -> None:
        pass

    def get_keys(self) -> list[int]:
        return list(self._store.keys())

    def touch(self, key: int) -> None:
        pass

    def try_get_value(self, key: ID) -> tuple[bool, int | str]:
        pass
