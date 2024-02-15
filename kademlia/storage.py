import os
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
        Returns a Boolean stating whether a key-value pair exists, given key.
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
        """
        Returns when the key was last republished as a datetime object.
        :param key:
        :return:
        """
        return datetime.fromisoformat(self._store[key]["republish_timestamp"])

    def set(self, key: ID, value: str, expiration_time_sec: int = 0) -> None:
        """
        Stores a key value pair, along with the expiration time and timestamp.
        :param key:
        :param value:
        :param expiration_time_sec:
        :return:
        """
        self._store[key.value] = StoreValue(value=value,
                                            expiration_time=expiration_time_sec,
                                            republish_timestamp=datetime.now().isoformat()
                                            )
        self.touch(key.value)

    def get_expiration_time_sec(self, key: int) -> int:
        """
        Returns how long it takes for the given key-value pair to expire, given key.
        :param key:
        :return:
        """
        return self._store[key]["expiration_time"]

    def remove(self, key: int) -> None:
        """
        Removes a given key-value pair, given key.
        :param key:
        :return:
        """
        if key in self._store:
            self._store.pop(key, None)

    def get_keys(self) -> list[int]:
        """
        Returns all keys of key-value pairs that are stored.
        :return:
        """
        return list(self._store.keys())

    def touch(self, key: int) -> None:
        """
        “touches” a given key-value pair, this is done by updating the timestamp to the current time.
        :param key:
        :return:
        """
        self._store[key]["republish_timestamp"] = datetime.now().isoformat()

    def try_get_value(self, key: ID) -> tuple[bool, str | None]:
        """
        Tries to get a given value from a key-value pair, given the key. Returns True | False, and the value if it was found.
        :param key:
        :return:
        """
        val: Optional[str] = None
        ret = False
        if key.value in self._store:
            val = self._store[key.value]["value"]
            ret = True

        return ret, val


class SecondaryJSONStorage(IStorage):
    def __init__(self, filename: str):
        """
        Storage object which reads/writes to a JSON file instead of to memory like how VirtualStorage does.
        the JSON is formatted as dict[int, StoreValue].

        This suffers from the drawbacks of using the JSON library; it writes the entire JSON to memory to read it,
        this may lead to heap errors. TODO: Do something about this (ijson might work?)

        Another drawback of this is that this will not be saved by DHT.save() - so all files stored inside this object
        would be lost!  # TODO: Fix this.

        :param filename: Filename to save values to - must end in .json!
        """
        self.filename = filename
        if not os.path.exists(self.filename):
            cwd = os.getcwd()
            if not os.path.exists(os.path.join(cwd, os.path.dirname(self.filename))):
                os.mkdir(os.path.join(cwd, os.path.dirname(self.filename)))
            with open(self.filename, "w"):
                pass  # Makes file.

    def set(self, key: ID, value: str | bytes, expiration_time_sec: int = 0) -> None:
        """
        Sets a key-value pair in the JSON along with the expiration time in seconds,
        and the timestamp as the current time. The python JSON library cannot store
        datetime objects, so it is converted in and out of “ISOFormat” which is a
        string representation of it.
        :param key:
        :param value:
        :param expiration_time_sec:
        :return:
        """
        os.makedirs(os.path.dirname(self.filename), exist_ok=True)
        with open(self.filename, "r") as f:
            print(f"Set at {self.filename}.")
            try:
                json_data: dict = json.load(f)
            except json.JSONDecodeError:
                json_data = {}

        to_store: StoreValue = StoreValue(
            value=value,
            expiration_time=expiration_time_sec,
            republish_timestamp=datetime.now().isoformat()
        )
        if key.value in json_data:
            json_data.pop(key.value)
        if str(key.value) in json_data:
            json_data.pop(str(key.value))

            print(json_data)
        json_data[key.value] = to_store
        print(json_data)

        with open(self.filename, "w") as f:
            json.dump(json_data, f)

    def contains(self, key: ID | int) -> bool:
        """
        Returns if the storage file contains a key-value pair, given the key.
        :param key:
        :return:
        """
        with open(self.filename, "r") as f:
            print(f"Contains at {self.filename}")
            f.seek(0)
            try:
                json_data: dict[int, StoreValue] = json.load(f)
            except json.JSONDecodeError as e:
                print(e)
                json_data = {}

        if isinstance(key, ID):
            return str(key.value) in list(json_data.keys())
        else:
            return str(key) in list(json_data.keys())

    def get_timestamp(self, key: int | ID) -> datetime:
        """
        Gets the timestamp of a key-value pair, given the key.
        :param key:
        :return:
        """
        with open(self.filename, "r") as f:
            print(f"Get timestamp at {self.filename}")
            try:
                json_data: dict[int, StoreValue] = json.load(f)
            except json.JSONDecodeError as e:
                print(e)
                json_data = {}
        if isinstance(key, ID):
            return datetime.fromisoformat(json_data[key.value]["republish_timestamp"])
        else:
            return datetime.fromisoformat(json_data[key]["republish_timestamp"])

    def get(self, key: ID | int) -> str:
        """
        Returns the value of a key-value pair from the storage file, given the key.
        :param key:
        :return:
        """
        with open(self.filename, "r") as f:
            f.seek(0)
            print(f"Get at {self.filename}")
            # try:
            json_data: dict = json.load(f)
            print("fdata", json_data)
            # except json.JSONDecodeError as e:
            #     print(e)
            #    json_data = {}
        if isinstance(key, ID):
            return json_data[str(key.value)]["value"]
        elif isinstance(key, int):
            return json_data[str(key)]["value"]
        else:
            raise TypeError("'get()' parameter 'key' must be type ID or int.")

    def get_expiration_time_sec(self, key: int) -> int:
        """
        Gets the time to expire for a key-value pair, given the key.
        :param key:
        :return:
        """
        with open(self.filename, "r") as f:
            print(f"Get expiration time at {self.filename}")
            try:
                json_data: dict[int, StoreValue] = json.load(f)
            except json.JSONDecodeError:
                json_data = {}
        return json_data[key]["expiration_time"]

    def remove(self, key: int) -> None:
        """
        Removes a key-value pair, given the key.
        :param key:
        :return:
        """
        with open(self.filename, "r") as f:
            print(f"Remove at {self.filename}")
            try:
                json_data: dict[str, StoreValue] = json.load(f)
            except json.JSONDecodeError:
                json_data = {}

        if str(key) in json_data:
            json_data.pop(str(key), None)

        with open(self.filename, "w") as f:
            json.dump(json_data, f)

    def get_keys(self) -> list[int]:
        """
        Returns all keys stored by the storage file as a list of integers.
        :return:
        """
        with open(self.filename, "r") as f:
            print(f"Get keys at {self.filename}")
            try:
                json_data: dict[int, StoreValue] = json.load(f)
            except json.JSONDecodeError:
                json_data = {}
            return list(json_data.keys())

    def touch(self, key: int | ID) -> None:
        """
        “touches” a key-value pair by setting the timestamp to the current time.
        :param key:
        :return:
        """
        with open(self.filename, "r") as f:
            print(f"Touch at {self.filename}")
            try:
                json_data: dict[int, StoreValue] = json.load(f)
            except json.JSONDecodeError:
                json_data = {}
            if isinstance(key, ID):
                json_data[key.value]["republish_timestamp"] = datetime.now().isoformat()
            else:
                json_data[key]["republish_timestamp"] = datetime.now().isoformat()
        with open(self.filename, "w") as f:
            json.dump(json_data, f)

    def try_get_value(self, key: ID) -> tuple[bool, int | str]:

        with open(self.filename, "r") as f:
            print(f"Try get value at {self.filename}")
            try:
                f.seek(0)
                print("File:", f.read())
                f.seek(0)
                # Key is a string because JSON library stores integers at strings
                json_data: dict[str,  StoreValue] = json.load(f)
                print(json_data)
            except json.JSONDecodeError as e:
                print(e)
                json_data = {}
        val = None
        ret = False
        if str(key.value) in json_data:
            val = json_data[str(key.value)]["value"]
            ret = True
        if json_data != {}:
            with open(self.filename, "w") as f:
                json.dump(json_data, f)

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
            print(f"Set file at {self.filename}")
            file_data = f.read()
        data_dict = {"filename": filename, "file_data": file_data}
        encoded_data: bytes = pickler.plain_encode_data(data=data_dict)
        encoded_data_str = encoded_data.decode("latin1")
        self.set(
            key=key,
            value=encoded_data_str,
            expiration_time_sec=expiration_time_sec
        )
