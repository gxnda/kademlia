"""
This is so incomplete; I'm not sure if it's worth the effort (It would allow for GUI unit tests though)
Might remove at some point, I guess it's something to do in comp sci lessons
for the time being.
"""
import os
import pickle
import re
import threading
import json
from os.path import exists, isfile
import argparse


import json
from typing import Callable
from requests import get

from kademlia import dht, id, networking, protocols, node, contact, storage, routers, errors, helpers
from kademlia.constants import Constants


parser = argparse.ArgumentParser()
parser.add_argument("--use_global_ip", action="store_true",
                    help="If the clients global IP should be used by the P2P network.")
parser.add_argument("--port", type=int, required=False, default=7124)

args = parser.parse_args()

USE_GLOBAL_IP = args.use_global_ip
PORT = args.port


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
        except OSError:
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


class MainGUI(GenericMenu):
    def __init__(self):
        GenericMenu.__init__(self, title="Kademlia")

        # Create our contact - this should be overwritten if bootstrapping.
        # self.initialise_kademlia()

        self.make_join_dht_frame()

    def initialise_kademlia(self):
        """
        Creates DHT, server and server thread. If GET-GLOBAL-IP is true, then it will get our global IP by
        decoding a response from ‘https://api.ipify.org’, which according to a StackOverflow article is the
        most efficient way to get your global IP in python. If GET-GLOBAL-IP is false, IP is set to “127.0.0.1”.
        This is useful for DHTs on just the local networks, so no port forwarding needs to be set up.

        Then a valid port is attempted to be found by getting a random integer between 5000 and 35000,
        until the port is free. This allows for multiple instances on one device, because the port is not
        hard coded. A TCPProtocol is created with this IP and Protocol. Then a contact is created with a
        random ID, and the protocol we just created. A JSON storage object is setup for main storage, and
        VirtualStorage for cache storage. These are placed into a subfolder with title “id.value”.

        Now we have initialised Kademlia, the main network frame is launched.

        :return:
        """
        print("[Initialisation] Initialising Kademlia.")

        our_id = id.ID.random_id()
        if USE_GLOBAL_IP:  # Port forwarding is required.
            our_ip = get('https://api.ipify.org').content.decode('utf8')
        else:
            our_ip = "127.0.0.1"
        print(f"[Initialisation] Our hostname is {our_ip}.")

        if PORT:
            valid_port = PORT
        else:
            valid_port = helpers.get_valid_port()

        print(f"[Initialisation] Port free at {valid_port}, creating our node here.")

        protocol = protocols.TCPProtocol(
            url=our_ip, port=valid_port
        )

        our_node = node.Node(
            contact=contact.Contact(
                id=our_id,
                protocol=protocol
            ),
            storage=storage.SecondaryJSONStorage(f"{our_id.value}/node.json"),
            cache_storage=storage.VirtualStorage()
        )

        # Make directory of our_id at current working directory.
        create_dir_at = os.path.join(os.getcwd(), str(our_id.value))
        print("[GUI] Making directory at", create_dir_at)
        if not exists(create_dir_at):
            os.mkdir(create_dir_at)

        self.dht: dht.DHT = dht.DHT(
            id=our_id,
            protocol=protocol,
            originator_storage=storage.SecondaryJSONStorage(f"{our_id.value}/originator_storage.json"),
            republish_storage=storage.SecondaryJSONStorage(f"{our_id.value}/republish_storage.json"),
            cache_storage=storage.VirtualStorage(),
            router=routers.ParallelRouter(our_node)
        )

        self.server = networking.TCPServer(our_node)
        self.server_thread = self.server.thread_start()

        self.make_network_frame()

    def open_settings(self):
        if hasattr(self, "dht"):
            settings_window = Settings(parent=self, hash_table=self.dht)
            settings_window.display_all()
        else:
            pass



    def clear_screen_and_keep_settings(self):
        self.add_settings_icon()

    def make_join_dht_frame(self):
        self.clear_screen_and_keep_settings()
        join = JoinNetworkMenu(parent=self)
        join.display_all()

    def make_load_dht_frame(self):
        self.clear_screen_and_keep_settings()
        load_dht = LoadDHTFromFileMenu(parent=self)
        load_dht.pack(padx=20, pady=20)

    def make_bootstrap_frame(self):
        self.clear_screen_and_keep_settings()
        bootstrap = BootstrapFrame(parent=self)
        bootstrap.pack(padx=20, pady=20)

    def make_bootstrap_from_json_frame(self):
        self.clear_screen_and_keep_settings()
        bootstrap_from_json = BootstrapFromJSONFrame(parent=self)
        bootstrap_from_json.pack(padx=20, pady=20)

    def make_network_frame(self):
        """
        Main network page
        I want this to have the following buttons:
        - Download file
        - Add new file
        :return:
        """
        self.clear_screen_and_keep_settings()
        network_frame = MainNetworkFrame(self)
        network_frame.pack(padx=20, pady=20)

    @classmethod
    def show_error(cls, error_message: str):
        print(f"[Error] {error_message}")
        error_window = ErrorWindow(error_message)
        error_window.mainloop()

    @classmethod
    def show_status(cls, message: str, copy_data=None):
        print(f"[Status] {message}")
        status_window = StatusWindow(message, copy_data)
        status_window.mainloop()

    def make_download_frame(self):
        self.clear_screen_and_keep_settings()
        download_frame = DownloadFrame(self)
        download_frame.pack(padx=20, pady=20)

    def make_upload_frame(self):
        self.clear_screen_and_keep_settings()
        upload_frame = UploadFrame(self)
        upload_frame.pack(padx=20, pady=20)
