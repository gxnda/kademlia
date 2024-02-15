import os
from hashlib import sha1
import random

from kademlia.contact import Contact
from kademlia.main import DEBUG
from kademlia.node import Node
from kademlia.id import ID
from kademlia.storage import VirtualStorage

TRY_CLOSEST_BUCKET = False  # TODO: Find somewhere good to put this / remove it entirely.

if DEBUG:
    random.seed(2)  # For consistent testing


def empty_node():
    """
    For testing.
    :return:
    """
    return Node(Contact(id=ID(0)), storage=VirtualStorage())


def random_node():
    return Node(Contact(id=ID.random_id()), storage=VirtualStorage())


def select_random(arr: list, freq: int) -> list:
    return random.sample(arr, freq)


def get_closest_number_index(numbers, target):
    closest_index = 0
    closest_difference = abs(numbers[0] - target)

    for i in range(1, len(numbers)):
        difference = abs(numbers[i] - target)
        if difference < closest_difference:
            closest_difference = difference
            closest_index = i

    return closest_index


def convert_file_to_key(filename: str) -> ID:
    sha1_hash = sha1()
    with open(filename, 'rb') as file:
        while True:
            data = file.read(4096)  # Read data from the file in chunks
            if not data:
                break
            sha1_hash.update(data)  # Update the hash object with the read data
    digest = int(sha1_hash.hexdigest(), 16)
    return ID(digest)


def make_sure_filepath_exists(filename: str) -> None:
    if os.path.isabs(filename):
        print(f"[DEBUG] Path {filename} is absolute.")
        path = filename
    else:
        print(f"[DEBUG] Path {filename} is not absolute.")
        path = os.path.join(os.getcwd(), filename)
        print(f"[DEBUG] Absolute version is {path}")
    if not os.path.exists(path):
        print(f"[DEBUG] Path does not exist.")
        dirname = os.path.dirname(path)
        if dirname:
            if not os.path.exists(dirname):
                os.mkdir(dirname)
    else:
        print("[DEBUG] Path already existed.")


# class ContactListAndError(TypedDict):
#     contacts: list[Contact]
#     error: RPCError
