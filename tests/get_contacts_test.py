import unittest
from ..kademlia import Contact, random_id_in_space, Node, ID


def get_close_contacts_ordered_test():
    sender: Contact = Contact(contact_ID=random_id_in_space(), protocol=None)
    node: Node = Node(Contact(contact_ID=random_id_in_space(), protocol=None), VirtualStorage())

    contacts: list[Contact] = []
    for i in range(100):
        contacts.append(Contact(contact_ID=random_id_in_space(), protocol=None))

    key: ID = random_id_in_space()

    
