import json
import logging
import os
import pickle
import re
from os.path import exists, isfile
from typing import Callable

from requests import get

import ui_helpers
from kademlia_dht import dht, contact, protocols, storage, networking, routers, node, helpers, id, pickler
from kademlia_dht.constants import Constants
from kademlia_dht.errors import IDMismatchError

USE_GLOBAL_IP, PORT, verbose = ui_helpers.handle_terminal()

logger: logging.Logger = ui_helpers.create_logger(verbose)


class GenericMenu:
    def __init__(self, title: str = "Generic Menu", parent=None, hash_table: dht.DHT | None = None):
        self.parent: GenericMenu | None = parent
        self.title = title
        self.__options: list[dict] = []
        self.__info: list = []
        if hash_table:
            self.dht: dht.DHT | None = hash_table
        else:
            self.dht: dht.DHT | None = self.parent.dht if self.parent else None

    def go_back(self):
        if self.parent:
            self.parent.display_all()
        else:
            logger.info("Error: No parent to go back to - orphan window.")
            self.display_all()

    def add_option(self, name: str, command: Callable, description: str = "") -> None:
        """
        Adds an option to a drop-down menu which will be displayed
        """
        if name in self.__options:
            raise ValueError(f"Option \"{name}\" is already in the option menu.")
        else:
            self.__options.append({"name": name, "command": command, "description": description})

    def add_info(self, info: str):
        self.__info.append(info)

    def get_input(self, prompt: str = ">> ", regex: str | None = None) -> str:
        user_input = input(prompt)
        if not regex:
            return user_input
        elif re.match(user_input, regex):
            return user_input
        else:
            logger.warning("Input was not valid, please try again.")
            return self.get_input(prompt, regex)

    def display(self) -> None:

        if self.__info or self.__options:
            print("\n\n--------", self.title, "--------\n")

        if self.__info:
            for line in self.__info:
                print(line)

        if self.__info and self.__options:
            print("\n")

        if self.__options:
            for i in range(len(self.__options)):
                print(f"{i + 1}) {self.__options[i]['name']}")
                if self.__options[i]['description']:
                    print(f"    Description: {self.__options[i]['description']}")

    def __get_choice(self) -> int:
        if self.__options:
            choice = input("Choice: ")

            if choice.isnumeric():
                if int(choice) - 1 in range(len(self.__options)):
                    return int(choice)
                else:
                    print("Choice out of range, please try again.")
                    return self.__get_choice()
            else:
                print("Choice was not a number, please try again.")
                return self.__get_choice()
        else:
            raise ValueError("There are no choices to be made - no options!")

    def __call_choice(self, choice: int) -> None:
        self.__options[choice - 1]["command"]()

    def display_all(self) -> None:
        self.display()
        if self.__options:
            choice = self.__get_choice()
            self.__call_choice(choice)


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
        self.add_option("Export our contact", self.export_contact)
        self.add_option("Back", self.go_back)
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
                         "(Leave blank for \"our_contact.json\" in project root directory):\n>> ")
        if not filename:
            filename = "our_contact.json"

        logger.info("Exporting our contact...")
        try:
            with open(filename, "w") as f:
                json.dump(contact_dict, f)
        except OSError:
            logger.error(f"Invalid filename \"{filename}\", please try again.")
            self.export_contact()

        logger.info(f"Successfully exported our contact to {filename}.")


class Settings(GenericMenu):
    def __init__(self, parent: GenericMenu):
        GenericMenu.__init__(self, title="Settings", parent=parent)
        # self.dht should be inherited by parent
        if self.dht:
            self.add_option("View or export our contact", self.open_contact_viewer)
        else:
            self.add_info("You have not made a DHT yet; you shouldn't be able to access this!")

        # Go back
        self.add_option("Back", parent.display_all)

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


class JoinNetworkMenu(GenericMenu):
    def __init__(self, parent: GenericMenu):
        GenericMenu.__init__(self, title="Join Network", parent=parent)

        self.add_option(name="Join stored network", command=self.make_load_dht_menu)
        self.add_option(name="Bootstrap into existing network", command=self.make_bootstrap_menu)
        self.add_option(name="Create new network", command=self.initialise_kademlia)

    def make_load_dht_menu(self):
        pass  # TODO: Create.

    def make_bootstrap_menu(self):
        pass  # TODO: Create.

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
        self.dht, self.server, self.server_thread = ui_helpers.initialise_kademlia(USE_GLOBAL_IP, PORT, logger=logger)

        self.make_main_network_menu()

    def make_main_network_menu(self):
        menu = MainNetworkMenu(parent=self)
        menu.display_all()


class BootstrapFromJSONMenu(GenericMenu):
    def __init__(self, parent: GenericMenu):
        raise NotImplementedError("Not implemented yet.")


class ManualBootstrapMenu(GenericMenu):
    def __init__(self, parent: GenericMenu):
        GenericMenu.__init__(self, title="Manual bootstrap", parent=parent)
        self.ip = self.get_input("Enter IP address of bootstrap peer (Leave blank to go back): ",
                       regex=r"(([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])\.){3}([0-9]|[1-9][0-9]|1[0-9]{2}|2["
                             r"0-4][0-9]|25[0-5])")
        if not self.ip:
            self.go_back()

        valid = False
        while not valid:
            self.port = self.get_input("Enter port of bootstrap peer (Leave blank to go back): ")
            if not self.port:
                self.go_back()
            elif self.port.isnumeric():
                valid = True
            else:
                logger.error("Port was not a number, please try again.")

        valid = False
        while not valid:
            self.id = self.get_input("Enter ID of bootstrap peer (Leave blank to go back): ")
            if not self.id:
                self.go_back()
            elif self.id.isnumeric():
                valid = True
            else:
                logger.error("ID was not a number, please try again.")

        valid = False
        while not valid:
            self.protocol = self.get_input("Enter protocol of bootstrap peer (TCP, TCPSubnet) (Leave blank to go back): ",
                                           regex=r"(TCP|TCPSubnet|tcpsubnet)")
            if not self.protocol:
                self.go_back()
            elif self.protocol.lower() == "tcpsubnet":
                self.protocol = protocols.TCPSubnetProtocol(self.ip, int(self.port), int(self.id))
            else:
                logger.error("Protocol was not recognised, please try again.")



class BootstrapMenu(GenericMenu):
    def __init__(self, parent: GenericMenu):
        GenericMenu.__init__(self, title="Bootstrap from known peer", parent=parent)
        self.add_option(name="Manual bootstrap", command=self.manual_bootstrap)
        self.add_option(name="Bootstrap from contact file", command=self.bootstrap_from_contact_json)

    def manual_bootstrap(self):
        menu = ManualBootstrapMenu(parent=self)
        menu.display_all()

    def bootstrap_from_contact_json(self):
        menu = BootstrapFromJSONMenu(parent=self)
        menu.display_all()


class MainNetworkMenu(GenericMenu):
    def __init__(self, parent: GenericMenu, has_settings=True):
        GenericMenu.__init__(self, parent=parent, title="Kademlia")

        self.add_option(name="Download", command=self.make_download_menu)
        self.add_option(name="Upload", command=self.make_upload_menu)

        if has_settings:
            self.add_option(name="Settings", command=self.open_settings)

    def open_settings(self):
        current_window = self
        menu = Settings(parent=current_window)
        menu.display_all()

    def make_download_menu(self):
        menu = DownloadMenu(parent=self)
        menu.display_all()

    def make_upload_menu(self):
        menu = UploadMenu(parent=self)
        menu.display_all()


class UploadMenu(GenericMenu):
    def __init__(self, parent: GenericMenu):
        GenericMenu.__init__(self, title="Upload File", parent=parent)

        user_input = self.get_input("File to upload (Leave blank to go back): ")
        if not user_input:
            self.go_back()
        else:
            self.file_to_upload = user_input
            self.handle_upload()

    def handle_upload(self):
        """
        Stores a file on our system on the network.
        This is stored by storing key: Random ID, value: Pickled dictionary
        {"filename": filename, "file": file_contents}
        The pickled dictionary is decoded using Constants.PICKLE_ENCODING (default: "latin1"), because the
        value sent must be a string.
        """
        file_to_upload = self.file_to_upload
        if isfile(file_to_upload):
            filename = os.path.basename(file_to_upload)
            if not filename:  # os.path.basename returns "" on file paths ending in "/"
                logger.error("File to upload must not be a directory.")
            else:
                ui_helpers.store_file(file_to_upload, self.parent.dht)
        else:
            logger.error(f"Path not found: {file_to_upload}")




class DownloadMenu(GenericMenu):
    def __init__(self, parent: GenericMenu):
        GenericMenu.__init__(self, title="Download File", parent=parent)

        valid = False
        while not valid:
            user_input = self.get_input("ID to download (Leave blank to go back): ")
            if not user_input:
                valid = True
                self.go_back()
            else:
                if not user_input:
                    logger.error("ID must not be empty, please try again.")
                elif not user_input.isnumeric():
                    logger.error("ID was not a number, please try again.")
                elif not 0 <= int(user_input) < 2 ** Constants.ID_LENGTH_BITS:
                    logger.error("ID out of range, please try again.")
                else:
                    valid = True
                    self.id_to_download = user_input
                    self.handle_download()

    def handle_download(self):
        id_to_download: id.ID = id.ID(int(self.id_to_download))
        try:
            download_path = ui_helpers.download_file(id_to_download, self.parent.dht)
            logger.info(f"File downloaded to {download_path}.")
        except IDMismatchError:
            logger.error("File ID not found on the network.")
        except Exception as e:
            logger.error(str(e))



if __name__ == "__main__":
    parent = GenericMenu()
    join_network_menu = JoinNetworkMenu(parent=parent)
    join_network_menu.display_all()
