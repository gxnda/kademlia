import random

from kademlia.contact import Contact
from main import DEBUG
from node import Node
from id import ID
from storage import VirtualStorage

TRY_CLOSEST_BUCKET = False  # TODO: Find somewhere good to put this / remove it entirely.

if DEBUG:
    random.seed(1)  # For consistent testing


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


# class ContactListAndError(TypedDict):
#     contacts: list[Contact]
#     error: RPCError

