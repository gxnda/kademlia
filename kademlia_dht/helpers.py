import os
from hashlib import sha1
import random
import socket

from kademlia_dht.constants import Constants
from kademlia_dht.contact import Contact
from kademlia_dht.node import Node
from kademlia_dht.id import ID
from kademlia_dht.storage import VirtualStorage


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
        if Constants.DEBUG:
            print(f"[DEBUG] Path {filename} is absolute.")
        path = filename
    else:
        if Constants.DEBUG:
            print(f"[DEBUG] Path {filename} is not absolute.")
        path = os.path.join(os.getcwd(), filename)
        if Constants.DEBUG:
            print(f"[DEBUG] Absolute version is {path}")
    if not os.path.exists(path):
        if Constants.DEBUG:
            print(f"[DEBUG] Path does not exist.")
        dirname = os.path.dirname(path)
        if dirname:
            if not os.path.exists(dirname):
                os.mkdir(dirname)
    else:
        if Constants.DEBUG:
            print("[DEBUG] Path already existed.")


def port_is_free(port: int) -> bool:
    """
    Returns if a port is free on localhost.
    :param port: Port to be checked
    :return: if it's free.
    """

    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.bind(('localhost', port))
            return True
    except (OSError, PermissionError):
        return False


def get_valid_port(default_tried=False,
                   lower_bound=1024, upper_bound=65535) -> int:
    """
    Gets a valid port on localhost.
    """
    if lower_bound > upper_bound:
        raise ValueError("Port lower bound cannot be greater than port upper bound.")

    if not default_tried:
        port = 7124  # Default port I wish to use.
        default_tried = True
    else:
        port = random.randint(lower_bound, upper_bound)
    if port_is_free(port):
        return port
    else:
        return get_valid_port(default_tried=default_tried)
