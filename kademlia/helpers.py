import random

from kademlia.contact import Contact
from kademlia.main import DEBUG
from kademlia.node import Node
from kademlia.id import ID
from kademlia.storage import VirtualStorage

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


def get_closest_number_index(numbers, target):
    closest_index = 0
    closest_difference = abs(numbers[0] - target)

    for i in range(1, len(numbers)):
        difference = abs(numbers[i] - target)
        if difference < closest_difference:
            closest_difference = difference
            closest_index = i

    return closest_index


# class ContactListAndError(TypedDict):
#     contacts: list[Contact]
#     error: RPCError
