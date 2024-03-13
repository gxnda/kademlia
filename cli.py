"""
TODO: This is so incomplete and I'm not sure if it's worth the effort
    (It would allow for GUI unit tests though)
Might remove at some point, I guess it's something to do in comp sci lessons
for the time being.
"""

import json
from typing import Callable

from kademlia import dht, contact, protocols


class GenericMenu:
    def __init__(self, title: str="Generic Menu", parent=None):
        self.parent: GenericMenu = parent
        self.title=title
        self.__options: list[dict] = []
        self.__info: list = []

    def go_back(self):
        if self.parent:
            self.parent.display_all()
        else:
            print("Error: No parent to go back to - orphan window.")
            self.display_all()

    def add_option(self, name: str, command: Callable, description: str = "") -> None:
        """
        Adds an option to a drop-down menu which will be displayed
        """
        if name in self.__options:
            raise ValueError(f"Option \"{name}\" is already in the option menu.")
        else:
            self.__options.append({"name": name, "command:": command, "description": description})

    def add_info(self, info: str):
        self.__info.append(info)
    
    def display(self) -> None:
        
        print("--------", self.title, "--------")
        
        if self.__info:
            for line in self.__info:
                print(line)
                
        if self.__info and self.__options:
            print("\n\n")
            
        if self.__options:
            for i in range(len(self.__options)):
                print(f"{i + 1}) {self.__options[i]['name']}")
                if self.__options[i]['description']:
                    print(f"    Description: {self.__options[i]['description']}")
                else:
                    print("\n")

    def __get_choice(self) -> int:
        if self.__options:
            choice = input("Choice: ")
            if choice.isnumeric():
                if int(choice) in range(len(self.__options)):
                    return int(choice)
                else:
                    print("Choice out of range, please try again.")
                    return self.__get_choice()
            else:
                print("Choice was not a number, please try again.")
                return self.__get_choice()
        else:
            raise ValueError("There are no choices to be made - no options!")

    def _call_choice(self, choice: int) -> None:
        self.__options[choice]["command"]()
    
    def display_all(self) -> int:
        self.display()
        return self.__get_choice()


class ContactViewer(GenericMenu):
    def __init__(self, parent: GenericMenu, id: int, protocol_type: type, url: str, port: int):
        GenericMenu.__init__(self, title="Our contact", parent=parent)
        self.id = id
        self.url = url
        self.port = port
        self.protocol_type = protocol_type
        self.add_info(f"ID: {self.id}")
        self.add_info(f"Protocol type: {self.protocol_type}")
        self.add_info(f"URL/IP: {self.url}")
        self.add_info(f"Port: {self.port}")
        self.add_option("Back", self.go_back)
        self.add_option("Export our contact", self.export_contact)
        self.display_all()

    def export_contact(self):

        contact_dict = {
            "url": self.url,
            "port": self.port,
            "protocol_type": str(self.protocol_type),
            "id": self.id
        }

        # Input validation for filename
        filename = input("Where should the file be saved to? "
                         "(Leave blank for \"our_contact\".json in root folder):\n>> ")
        if not filename:
            filename = "our_contact.json"

        print("Exporting our contact...")
        try:
            with open(filename, "w") as f:
                json.dump(contact_dict, f)
        except OSError as e:
            print(e)  # TODO: Remove.
            print(f"Invalid filename \"{filename}\", please try again.")
            self.export_contact()

        print(f"Exported our contact to {filename}.")


class Settings(GenericMenu):
    def __init__(self, parent: GenericMenu, hash_table: dht.DHT | None):
        GenericMenu.__init__(self, title="Settings", parent=parent)

        self.dht: dht.DHT | None = hash_table

        if self.dht:
            self.add_option("View or export our contact", self.open_contact_viewer)
        else:
            print("womp womp")  # TODO: Remove.
            self.add_info("You have not made a DHT yet; you shouldn't be able to access this!")

    def open_contact_viewer(self):
        our_contact: contact.Contact = self.dht.our_contact
        our_id: int = our_contact.id.value
        # noinspection PyTypeChecker
        protocol: protocols.TCPProtocol = our_contact.protocol
        protocol_type: type = type(protocol)
        our_ip_address: str = protocol.url
        our_port: int = protocol.port

        contact_viewer = ContactViewer(
            id=our_id,
            protocol_type=protocol_type,
            url=our_ip_address,
            port=our_port,
            parent=self
        )

        contact_viewer.display_all()
