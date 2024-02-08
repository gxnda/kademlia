from datetime import datetime
import json
from typing import Optional

from kademlia import pickler
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
        return key.value in self.get_keys()

    def get(self, key: ID | int) -> str:
        """
        Returns stored value, associated with given key value.
        :param key: Type ID or Integer, key value to be searched.
        :return:
        """
        if isinstance(key, ID):
            return self._store[key.value]["value"]
        elif isinstance(key, int):
            return self._store[key]["value"]
        else:
            raise TypeError("'get()' parameter 'key' must be type ID or int.")

    def get_timestamp(self, key: int) -> datetime:
        return self._store[key]["republish_timestamp"]

    def set(self, key: ID, value: str, expiration_time_sec: int = 0) -> None:
        self._store[key.value] = StoreValue(value=value,
                                            expiration_time=expiration_time_sec,
                                            republish_timestamp=datetime.now())
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
        val: Optional[str] = None
        ret = False
        if key.value in self._store:
            val = self._store[key.value]["value"]
            ret = True

        return ret, val


class SecondaryStorage(IStorage):
    def __init__(self, filename: str):
        """
        Storage object which reads/writes to a JSON file instead of to memory like how VirtualStorage does.
        the JSON is formatted as dict[int, StoreValue].

        This suffers from the drawbacks of using the JSON library; it writes the entire JSON to memory to read it,
        this may lead to heap errors. TODO: Do something about this (ijson might work?)

        :param filename: Filename to save values to - must end in .json!
        """
        self.filename = filename

    def set(self, key: ID, value: str | bytes, expiration_time_sec: int = 0) -> None:
        with open(self.filename) as f:
            json_data: dict[int, StoreValue] = json.load(f)
            to_store: StoreValue = StoreValue(
                value=value,
                expiration_time=expiration_time_sec,
                republish_timestamp=datetime.now()
            )
            json_data[key.value] = to_store

    def contains(self, key: ID) -> bool:
        with open(self.filename) as f:
            json_data: dict[int, StoreValue] = json.load(f)
            return key.value in json_data

    def get_timestamp(self, key: int) -> datetime:
        with open(self.filename) as f:
            json_data: dict[int, StoreValue] = json.load(f)
            return json_data[key]["republish_timestamp"]

    def get(self, key: ID | int) -> StoreValue:
        with open(self.filename) as f:
            json_data: dict[int, StoreValue] = json.load(f)
            if isinstance(key, ID):
                return json_data[key.value]
            elif isinstance(key, int):
                return json_data[key]
            else:
                raise TypeError("'get()' parameter 'key' must be type ID or int.")

    def get_expiration_time_sec(self, key: int) -> int:
        with open(self.filename) as f:
            json_data: dict[int, StoreValue] = json.load(f)
            return json_data[key]["expiration_time"]

    def remove(self, key: int) -> None:
        with open(self.filename) as f:
            json_data: dict[int, StoreValue] = json.load(f)
            if key in json_data:
                json_data.pop(key, None)

    def get_keys(self) -> list[int]:
        with open(self.filename) as f:
            json_data: dict[int, StoreValue] = json.load(f)
            return list(json_data.keys())

    def touch(self, key: int) -> None:
        with open(self.filename) as f:
            json_data: dict[int, StoreValue] = json.load(f)
            json_data[key]["republish_timestamp"] = datetime.now()

    def try_get_value(self, key: ID) -> tuple[bool, int | str]:
        with open(self.filename) as f:
            json_data: dict[int, StoreValue] = json.load(f)
        val = None
        ret = False
        if key.value in json_data:
            val = json_data[key.value]["value"]
            ret = True

        return ret, val

    def set_file(self, key: ID, filename: str, expiration_time_sec: int = 0) -> None:
        """
        Adds a file to storage file, it does this by loading ALL of the file to be added to memory,
        and then pasting it into the storage file ALSO loaded into memory D:
        :param key:
        :param filename:
        :param expiration_time_sec:
        :return:
        """
        with open(filename) as f:
            file_data = f.read()
        data_dict = {"filename": filename, "file_data": file_data}
        encoded_data: bytes = pickler.plain_encode_data(data=data_dict)
        encoded_data_str = encoded_data.decode("utf-8")
        self.set(
            key=key,
            value=encoded_data_str,
            expiration_time_sec=expiration_time_sec
        )
