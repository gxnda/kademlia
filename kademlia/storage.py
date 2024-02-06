from datetime import datetime

from kademlia.dictionaries import StoreValue
from kademlia.id import ID
from kademlia.interfaces import IStorage


class VirtualStorage(IStorage):
    """
    Simple storage mechanism that stores things in memory.
    """

    def __init__(self):
        self._store: dict[int, StoreValue] = {}

    def contains(self, key: ID) -> bool:
        """
        Returns a boolean stating whether a key is storing something.
        """
        return key.value in list(self._store.keys())

    def get(self, key: ID | int) -> StoreValue:
        """
        Returns stored value, associated with given key value.
        :param key: Type ID or Integer, key value to be searched.
        :return:
        """
        if isinstance(key, ID):
            return self._store[key.value]
        elif isinstance(key, int):
            return self._store[key]
        else:
            raise TypeError("'get()' parameter 'key' must be type ID or int.")

    def get_timestamp(self, key: int) -> datetime:
        return self._store[key]["republish_timestamp"]

    def set(self, key: ID, value: str, expiration_time_sec: int = 0) -> None:
        self._store[key.value] = StoreValue(value=value, expiration_time=expiration_time_sec)
        self.touch(key.value)

    def get_expiration_time_sec(self, key: int) -> int:
        return self._store[key]["expiration_time"]

    def remove(self, key: int) -> None:
        if key in self._store:
            self._store.pop(key, None)

    def get_keys(self) -> list[int]:
        return list(self._store.keys())

    def touch(self, key: int) -> None:
        self._store[key]["republish_timestamp"] = datetime.now()

    def try_get_value(self, key: ID) -> tuple[bool, int | str | None]:
        val = None
        ret = False
        if key.value in self._store:
            val = self._store[key.value]
            ret = True

        return ret, val
