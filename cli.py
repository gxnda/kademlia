import argparse
import json
import os
import pickle
import re
from os.path import exists, isfile
from typing import Callable

from requests import get

from kademlia import dht, contact, protocols, storage, networking, routers, node, helpers, id
from kademlia.constants import Constants

parser = argparse.ArgumentParser()
parser.add_argument("--use_global_ip", action="store_true",
                    help="If the clients global IP should be used by the P2P network.")
parser.add_argument("--debug", action="store_true",
                    help="If the clients global IP should be used by the P2P network.")
parser.add_argument("--port", type=int, required=False, default=7124)

args = parser.parse_args()

USE_GLOBAL_IP = args.use_global_ip
PORT = args.port
Constants.DEBUG = args.debug


class GenericMenu:
    def __init__(self, title: str="Generic Menu", parent=None, hash_table: dht.DHT | None = None):
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
            print("Error: No parent to go back to - orphan window.")
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
            print("Input was not valid, please try again.")
            return self.get_input(prompt, regex)
    
    def display(self) -> None:

        if self.__info or self.__options:
            print("\n--------", self.title, "--------\n")
        
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
    def __init__(self, parent: GenericMenu):
        GenericMenu.__init__(self, title="Settings", parent=parent)
        # self.dht should be inherited by parent
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

        self.make_main_network_menu()

    def make_main_network_menu(self):
        menu = MainNetworkMenu(parent=self)
        menu.display_all()


class BootstrapFromJSONMenu(GenericMenu):
    pass  # TODO: This should not be an object, this should be a function.


class BootstrapMenu(GenericMenu):
    pass  # TODO: This should not be an object, this should be a function.


class MainNetworkMenu(GenericMenu):
    def __init__(self, parent: GenericMenu):
        GenericMenu.__init__(self, parent=parent, title="Kademlia")

        self.add_option(name="Download", command=self.make_download_menu)
        self.add_option(name="Upload", command=self.make_upload_menu)

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
        file_to_upload = self.file_to_upload
        if isfile(file_to_upload):
            filename = os.path.basename(file_to_upload)
            if not filename:  # os.path.basename returns "" on file paths ending in "/"
                print("[ERROR] File to upload must not be a directory.")
            else:
                with open(file_to_upload, "rb") as f:
                    file_contents: bytes = f.read()

                # TODO: Make a function to do this.
                # val will be a 'latin1' pickled dictionary {filename: str, file: bytes}
                val: str = pickle.dumps({"filename": filename, "file": file_contents}).decode("latin1")
                del file_contents  # free up memory, file_contents could be pretty big.

                id_to_store_to = id.ID.random_id()
                self.parent.dht.store(id_to_store_to, val)
                print("[STATUS] Stored file at {id_to_store_to}.")
        else:
            print(f"[ERROR] Path not found: {file_to_upload}")


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
                    print("[ERROR] ID must not be empty, please try again.")
                elif not user_input.isnumeric():
                    print("[ERROR] ID was not a number, please try again.")
                elif not 0 <= int(user_input) < 2 ** Constants.ID_LENGTH_BITS:
                    print("[ERROR] ID out of range, please try again.")
                else:
                    valid = True
                    self.id_to_download = user_input
                    self.handle_download()

    def handle_download(self):
        id_to_download: id.ID = id.ID(int(self.id_to_download))
        found, contacts, val = self.parent.dht.find_value(key=id_to_download)
        # val will be a 'latin1' pickled dictionary {filename: str, file: bytes}
        if not found:
            print("[ERROR] File not found.")
        else:
            # TODO: It might be a better idea to use JSON to send values.
            val_bytes: bytes = val.encode("latin1")  # TODO: Add option for changing this in settings.

            # "pickle.loads()" is very insecure and can lead to arbitrary code execution, the val received
            #   could be maliciously crafted to allow for malicious code execution because it compiles and creates
            #   a python object.
            file_dict: dict = pickle.loads(val_bytes)  # TODO: Make secure.
            filename: str = file_dict["filename"]
            file_bytes: bytes = file_dict["file"]
            del file_dict  # Free up memory.

            cwd = os.getcwd()  # TODO: Add option to change where it is installed to.
            with open(os.path.join(cwd, filename), "wb") as f:
                f.write(file_bytes)

            print("[STATUS] File downloaded to {os.path.join(cwd, filename)}.")


if __name__ == "__main__":
    parent = GenericMenu()
    join_network_menu = JoinNetworkMenu(parent=parent)
    join_network_menu.display_all()