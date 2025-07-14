import json
import logging
import os
import struct
from contextlib import nullcontext
from datetime import datetime
from threading import RLock, Lock
from typing import Optional

from . import pickler
from .dictionaries import StoreValue
from .id import ID
from .interfaces import IStorage


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


class BinaryFileStorage(IStorage):
    """
    Stores in a directory structure, with each piece being a .kpiece file,
    which is just a binary file.

    The structure of each binary file must contain the following keys:
    Republish timestamp: Time when last republished, 45 bits
    Expiration time: number of seconds until expiration

    """
    def __init__(self, directory: str, add_existing=False):
        os.makedirs(directory, exist_ok=True)
        self.directory: str = directory
        self.locks: dict[int, Lock] = {}
        self.lock = nullcontext()

        # Add any existing pieces
        if add_existing:
            for filename in os.listdir(self.directory):
                if filename.endswith(".kpiece"):
                    filename_no_suffix, _, _ = filename.partition(".kpiece")
                    if filename_no_suffix.isnumeric():
                        self.locks[int(filename_no_suffix)] = Lock()

    @classmethod
    def convert_datetime_to_bytes(cls, dt: datetime) -> bytes:
        """
        Returns datetime (truncated to seconds) as an unsigned long in bytes.
        """
        return struct.pack(">d", dt.timestamp())

    @classmethod
    def convert_bytes_to_datetime(cls, b: bytes):
        return datetime.fromtimestamp(*struct.unpack(">d", b))

    def contains(self, key: ID | int) -> bool:
        if isinstance(key, ID):
            key = key.value
        return key in self.locks

    def try_get_value(self, key: ID | int) -> tuple[bool, str]:
        if isinstance(key, ID):
            key: int = key.value
        if not self.contains(key):
            return False, ""

        with (self.locks[key]):
            with open(os.path.join(self.directory, str(key)) + ".kpiece", "rb") as f:
                # republish_timestamp: datetime = self.convert_bytes_to_datetime(f.read(4))
                # exp_time_sec: int = struct.unpack(">L", f.read(4))[0]
                f.seek(12) # Skips over republish timestamp and exp_time
                val_is_bytes: bytes = struct.unpack("?", f.read(1))[0]
                value_size: int = struct.unpack(">L", f.read(4))[0]
                if val_is_bytes:
                    value: bytes = f.read(value_size)
                else:
                    value: str = f.read(value_size).decode("utf-8")

                return True, value

    def get(self, key: ID | int) -> Optional[str | bytes]:
        if isinstance(key, ID):
            key: int = key.value

        if not self.contains(key):
            return None

        with (self.locks[key]):
            with open(os.path.join(self.directory, str(key)) + ".kpiece", "rb") as f:
                # republish_timestamp: datetime = self.convert_bytes_to_datetime(f.read(4))
                # exp_time_sec: int = struct.unpack(">L", f.read(4))[0]
                f.seek(12) # Skips over republish timestamp and exp_time
                val_is_bytes: bytes = struct.unpack("?", f.read(1))[0]
                value_size: int = struct.unpack(">L", f.read(4))[0]
                if val_is_bytes:
                    value: bytes = f.read(value_size)
                else:
                    value: str = f.read(value_size).decode("utf-8")

                return value

    def get_timestamp(self, key: ID | int) -> datetime:
        if isinstance(key, ID):
            key: int = key.value

        if not self.contains(key):
            raise KeyError("Key is not in Binary file storage.")

        with (self.locks[key]):
            with open(os.path.join(self.directory, str(key)) + ".kpiece", "rb") as f:
                f.seek(0)
                return self.convert_bytes_to_datetime(f.read(8))

    def get_expiration_time_sec(self, key: ID | int) -> int:
        if isinstance(key, ID):
            key: int = key.value

        if not self.contains(key):
            raise KeyError("Key is not in Binary file storage.")

        with (self.locks[key]):
            with open(os.path.join(self.directory, str(key)) + ".kpiece", "rb") as f:
                f.seek(8)
                return struct.unpack(">L", f.read(4))[0]

    def remove(self, key: ID | int) -> None:
        if isinstance(key, ID):
            key: int = key.value

        if not self.contains(key):
            raise KeyError("Key is not in Binary file storage.")

        with self.locks[key]:
            os.unlink(os.path.join(self.directory, str(key) + ".kpiece"))
            self.locks.pop(key)

    def get_keys(self) -> list[int]:
        return list(self.locks.keys())

    def touch(self, key: ID | int) -> None:
        if isinstance(key, ID):
            key: int = key.value

        if not self.contains(key):
            raise KeyError("Key is not in Binary file storage.")

        republish_timestamp: bytes = self.convert_datetime_to_bytes(
            datetime.now()
        )  # 4 bytes
        with (self.locks[key]):
            with open(os.path.join(self.directory, str(key)) + ".kpiece", "r+b") as f:
                # Set republish_timestamp to now
                f.seek(0)
                f.write(republish_timestamp)

    def set(self, key: ID, value: str | bytes, expiration_time_sec: int = 0):
        if expiration_time_sec < 0:
            raise ValueError(
                "Cannot have negative expiration time for stored key-value."
            )
        if not key.value in self.locks:
            self.locks[key.value] = Lock()

        republish_timestamp: bytes = self.convert_datetime_to_bytes(
            datetime.now()
        ) # 4 bytes

        val_is_bytes = isinstance(value, bytes)
        with self.locks[key.value]:
            with open(os.path.join(self.directory, str(key.value)) + ".kpiece",
                      "wb") as f:
                f.seek(0)
                f.write(republish_timestamp)
                f.write(struct.pack(">L", expiration_time_sec))
                f.write(struct.pack("?", val_is_bytes))
                if val_is_bytes:
                    f.write(
                        struct.pack(">L", len(value))
                    )
                    f.write(value)
                else: # If val is string
                    encoded = value.encode('utf-8')
                    f.write(
                        struct.pack(">L", len(encoded)))
                    f.write(encoded)


if __name__ == "__main__":
    dt = datetime.now()
    flt = struct.pack(">d", dt.timestamp())
    print(flt, len(flt))