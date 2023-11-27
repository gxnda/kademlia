from network import Contact, BucketList, Router
from abc import abstractmethod
from datetime import datetime


class ID:
    def __init__(self, value: int):
        """
        Kademlia node ID: This is an integer from 0 to 2^160 - 1

        Args:
            value: (int) ID denary value
        """
        
        two_160 = 1461501637330902918203684832716283019655932542976
        self.MAX_ID = two_160 # 2^160
        self.MIN_ID = 0
        if not (value < self.MAX_ID and value > self.MIN_ID):
            raise ValueError("ID out of range - must a positive integer less "\
            "than 2^160.")
        self.value = value

    def hex(self) -> str:
        return hex(self.value)

    def denary(self) -> int:
        return self.value
    
    def bin(self) -> str:
        return bin(self.value)
    
    def __str__(self) -> str:
        return str(self.value)


class IStorage:
    """Interface which 'abstracts the storage mechanism for key-value pairs.''"""
    
    @abstractmethod
    def contains(self, key: ID) -> bool:
        pass

    @abstractmethod
    def try_get_value(self, key: ID, out: (int or str)) -> bool:
        pass

    @abstractmethod
    def get(self, key: (ID or int)) -> str:
        pass

    @abstractmethod
    def get_timestamp(self, key: int) -> datetime:
        pass

    @abstractmethod
    def set(self, key: ID, value: str, expiration_time_sec: int=0) -> None:
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
    


class Node:
    def __init__(self, contact: Contact, storage: IStorage, cache_storage=None):
        self._ourContact: Contact = contact
        self._bucket_list = BucketList(contact.id)
        self._storage: IStorage = storage

    def ping(self, sender: Contact) -> Contact:
        # !!! TO BE IMPLEMENTED
        pass

    def store(self, sender: Contact, key: ID, value: str) -> None:
        # !!! TO BE IMPLEMENTED
        pass

    def find_node(self, sender: Contact, key: ID): # -> (list[Contact], str)
        # !!! TO BE IMPLEMENTED
        pass

    def find_value(self, sender: Contact, key: ID): # -> (list[Contact], str)
        # !!! TO BE IMPLEMENTED
        pass


class DHT:
    def __init__(self):
        self._base_router = None

    def router(self):
        return self._base_router


    
        

if __name__ == "__main__":
    pass
    