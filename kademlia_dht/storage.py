import json
import logging
import os
from datetime import datetime
from threading import RLock

from kademlia_dht import pickler
from kademlia_dht.dictionaries import StoreValue
from kademlia_dht.id import ID
from kademlia_dht.interfaces import IStorage


logger = logging.getLogger("__main__")


class VirtualStorage(IStorage):
    def __init__(self):
        self._store: dict[int, StoreValue] = {}
        self.lock = RLock()

    # --- Locked Methods ---
    def contains(self, key: ID) -> bool:
        with self.lock:
            return key.value in self._store

    def get(self, key: ID | int) -> str:
        with self.lock:
            if isinstance(key, ID):
                return self._store[key.value]["value"]
            elif isinstance(key, int):
                return self._store[key]["value"]
            else:
                raise TypeError("Key must be ID or int.")

    def get_timestamp(self, key: int) -> datetime:
        with self.lock:
            return datetime.fromisoformat(self._store[key]["republish_timestamp"])

    def set(self, key: ID, value: str, expiration_time_sec: int = 0) -> None:
        with self.lock:
            self._store[key.value] = {
                "value": value,
                "expiration_time": expiration_time_sec,
                "republish_timestamp": datetime.now().isoformat()
            }

    def get_expiration_time_sec(self, key: int) -> int:
        with self.lock:
            return self._store[key]["expiration_time"]

    def remove(self, key: int) -> None:
        with self.lock:
            self._store.pop(key, None)

    def get_keys(self) -> list[int]:
        with self.lock:
            return list(self._store.keys())

    def touch(self, key: int) -> None:
        with self.lock:
            self._store[key]["republish_timestamp"] = datetime.now().isoformat()

    def try_get_value(self, key: ID) -> tuple[bool, str | None]:
        with self.lock:
            val = self._store.get(key.value)
            return val is not None, val["value"] if val else None

    # --- Unlocked Methods ---
    def __repr__(self) -> str:
        return f"VirtualStorage(keys={list(self._store.keys())})"


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
        self.lock = RLock()
        with self.lock:
            if not os.path.exists(self.filename):
                if not os.path.exists(os.path.dirname(self.filename)):
                    os.makedirs(os.path.dirname(self.filename))
                with open(self.filename, "w") as f:
                    f.write("{}")

    def __repr__(self):
        return str({
            "type": "SecondaryJSONStorage",
            "filename": self.filename
        })

    def __str__(self):
        return str({
            "type": "SecondaryJSONStorage",
            "filename": self.filename
        })

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
        to_store: StoreValue = StoreValue(
            value=value,
            expiration_time=expiration_time_sec,
            republish_timestamp=datetime.now().isoformat()
        )

        with self.lock:
            os.makedirs(os.path.dirname(self.filename), exist_ok=True)
            with open(self.filename, "r") as f:
                logger.info(f"Set at {self.filename}.")
                try:
                    json_data: dict = json.load(f)
                except json.JSONDecodeError:
                    json_data = {}

            if key.value in json_data:
                json_data.pop(key.value)

            if str(key.value) in json_data:
                json_data.pop(str(key.value))

            json_data[int(key.value)] = to_store

            with open(self.filename, "w") as f:
                json.dump(json_data, f)

    def contains(self, key: ID | int) -> bool:
        """
        Returns if the storage file contains a key-value pair, given the key.
        :param key:
        :return:
        """
        with self.lock:
            with open(self.filename, "r") as f:
                logger.debug(f"Contains key \"{key}\" at {self.filename}")
                f.seek(0)
                try:
                    json_data: dict[int, StoreValue] = json.load(f)
                except json.JSONDecodeError as e:
                    logger.error(f"JSON decoding error in 'contains' for {self.filename}: {e}")
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
        with self.lock:
            with open(self.filename, "r") as f:
                logger.debug(f"Get timestamp at {self.filename}")
                try:
                    json_data: dict[int, StoreValue] = json.load(f)
                except json.JSONDecodeError as e:
                    logger.error(f"JSON decoding error in 'get_timestamp' for {self.filename}: {e}")
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
        with self.lock:
            with open(self.filename, "r") as f:
                f.seek(0)
                logger.debug(f"Get at {self.filename}")
                json_data: dict = json.load(f)
                logger.debug("fdata", json_data)

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
        with self.lock:
            with open(self.filename, "r") as f:
                logger.debug(f"Get expiration time at {self.filename}")
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
        with self.lock:
            with open(self.filename, "r") as f:
                logger.debug(f"Remove at {self.filename}")
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
        with self.lock:
            with open(self.filename, "r") as f:
                logger.debug(f"Get keys at {self.filename}")
                try:
                    json_data: dict[int, StoreValue] = json.load(f)
                except json.JSONDecodeError:
                    json_data = {}
                return [int(k) for k in list(json_data.keys())]

    def touch(self, key: int | ID) -> None:
        """
        “touches” a key-value pair by setting the timestamp to the current time.
        :param key:
        :return:
        """
        with self.lock:
            with open(self.filename, "r") as f:
                logger.debug(f"Touch at {self.filename}")
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
        with self.lock:
            with open(self.filename, "r") as f:
                logger.debug(f"Try get value at {self.filename}")
                try:
                    f.seek(0)
                    # Key is a string because JSON library stores integers at strings
                    json_data: dict[str, StoreValue] = json.load(f)
                except json.JSONDecodeError as e:
                    logger.error(f"JSON decoding error in 'try_get_value' for {self.filename}: {e}")
                    json_data = {}

        val = None
        ret = False
        if str(key.value) in json_data:
            val = json_data[str(key.value)]["value"]
            ret = True

        return ret, val

    def set_file(self, key: ID, filename: str, expiration_time_sec: int = 0) -> None:
        """
        Adds a file to storage file, it does this by loading ALL the file to be added to memory,
        and then pasting it into the storage file ALSO loaded into memory D:
        :param key:
        :param filename:
        :param expiration_time_sec:
        :return:
        """
        with open(filename) as f:
            logger.debug(f"Adding data to JSON storage in{self.filename}")
            file_data = f.read()
        data_dict = {"filename": filename, "file_data": file_data}
        encoded_data_str = pickler.encode_data(data=data_dict)
        self.set(
            key=key,
            value=encoded_data_str,
            expiration_time_sec=expiration_time_sec
        )
