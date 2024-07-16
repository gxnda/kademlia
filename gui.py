import argparse
import json
import logging
import os
import pickle
import re
import threading
from os.path import exists, isfile
from sys import stdout

import customtkinter as ctk
from PIL import Image
from requests import get

import ui_helpers
from kademlia_dht import dht, id, networking, protocols, node, contact, storage, routers, errors, helpers, pickler
from kademlia_dht.constants import Constants

"""
├── User Interface
│   ├── Join
│   │   ├── Settings
│   │   ├── Load an existing network
│   │   └── Bootstrap into a new network
│   ├── Main menu
│   │   ├── Settings
│   │   ├── Download a file
│   │   ├── Add a file for upload
│   │   ├── Remove a file for upload
│   │   ├── Leave network
│   │   └── Search using key


open UserInterface(), then check if the user is in a network already or not
- the k-buckets are stored in a JSON file, which is used to check this, 
and if the user is not in a network, the user is prompted to join a network.
"""


USE_GLOBAL_IP, PORT, verbose = ui_helpers.handle_terminal()

logger: logging.Logger = ui_helpers.create_logger(verbose)


class Fonts:
    title_font = ("Segoe UI", 20, "bold")
    text_font = ("Segoe UI", 16)


class ContactViewer(ctk.CTk):
    def __init__(self, id: int, protocol_type: type, url: str, port: int, appearance_mode="dark"):
        super().__init__()
        ctk.set_appearance_mode(appearance_mode)

        self.id = id
        self.url = url
        self.port = port
        self.protocol_type = protocol_type

        self.contact_viewer_title = ctk.CTkLabel(self, text="Our Contact:", font=Fonts.title_font)
        self.contact_viewer_title.pack(padx=20, pady=30)

        self.id_label = ctk.CTkLabel(self, text=f"ID: {self.id}", font=Fonts.text_font)
        self.id_label.pack(padx=20, pady=10)

        self.protocol_type_label = ctk.CTkLabel(self, text=f"Protocol type: {self.protocol_type}", font=Fonts.text_font)
        self.protocol_type_label.pack(padx=20, pady=10)

        self.url_label = ctk.CTkLabel(self, text=f"URL: {self.url}", font=Fonts.text_font)
        self.url_label.pack(padx=20, pady=10)

        self.port_label = ctk.CTkLabel(self, text=f"Port: {self.port}", font=Fonts.text_font)
        self.port_label.pack(padx=20, pady=10)

        self.export_button = ctk.CTkButton(self, text="Export our contact", font=Fonts.text_font,
                                           command=self.export_contact)
        self.export_button.pack(padx=20, pady=10)

    def show_error(self, error_message: str):
        logger.error(f"[Error] {error_message}")

        error_window = ErrorWindow(error_message)
        error_window.mainloop()

    def export_contact(self, filename="our_contact.json"):
        contact_dict = {
            "url": self.url,
            "port": self.port,
            "protocol_type": str(self.protocol_type),
            "id": self.id
        }
        logger.info(f"Exporting our contact...")
        with open(filename, "w") as f:
            json.dump(contact_dict, f)
        self.show_status(f"Exported our contact to {filename}.")

    def show_status(self, message: str):
        logger.info(message)
        status_window = StatusWindow(message)
        status_window.mainloop()


class StatusWindow(ctk.CTk):
    def __init__(self, message: str, copy_data=None):
        """
        Creates the status window, with option for copying data to clipboard if there is copy data.
        :param message:
        :param copy_data:
        """
        ctk.CTk.__init__(self)
        self.copy_data = copy_data
        self.message = ctk.CTkLabel(self, text=message, font=Fonts.text_font)
        self.message.pack(padx=30, pady=20)
        self.copy_button = ctk.CTkButton(self, text="Copy to clipboard", font=Fonts.text_font,
                                         command=self.copy)
        if copy_data:
            self.copy_button.pack(padx=30, pady=20)

    def copy(self):
        logger.info(f"Copying data to clipboard: {self.copy_data}")
        self.clipboard_clear()
        self.clipboard_append(self.copy_data)
        self.update()


class Settings(ctk.CTk):
    def __init__(self, hash_table: dht.DHT | None, appearance_mode="dark"):
        super().__init__()
        ctk.set_appearance_mode(appearance_mode)

        self.appearance_mode = appearance_mode

        self.dht: dht.DHT | None = hash_table

        self.title("Kademlia Settings")

        self.settings_title = ctk.CTkLabel(self, text="Settings", font=Fonts.title_font)
        self.settings_title.grid(column=0, row=0, columnspan=2, padx=20, pady=20)

        if self.dht:
            self.dht_export_label = ctk.CTkLabel(self, text="File to export to:", width=150, font=Fonts.text_font)
            self.dht_export_label.grid(column=0, row=1, padx=10, pady=10)

            self.dht_export_file = ctk.CTkEntry(self, width=200, height=20, font=Fonts.text_font)
            self.dht_export_file.grid(column=1, row=1, padx=10, pady=10)
            self.dht_export_file.insert("1", f"dht.pickle")
            self.export_dht_button = ctk.CTkButton(self, text="Export/Save DHT", font=Fonts.text_font,
                                                   command=self.export_dht)
            self.export_dht_button.grid(column=1, row=2, padx=10, pady=10)

            self.view_contact_button = ctk.CTkButton(self, text="View our contact", font=Fonts.text_font,
                                                     command=self.view_contact)
            self.view_contact_button.grid(column=0, row=2, padx=10, pady=10)
        else:
            no_dht_label = ctk.CTkLabel(self,
                                        text="You have not made a DHT yet! You should not be able to access this.")
            no_dht_label.grid(column=0, row=1, padx=10, pady=10)

    def show_error(self, error_message: str):
        logger.error(error_message)
        error_window = ErrorWindow(error_message)
        error_window.mainloop()

    def show_status(self, message: str):
        logger.info(message)
        status_window = StatusWindow(message)
        status_window.mainloop()

    def export_dht(self):
        try:
            file = self.dht_export_file.get().strip("\n")
            self.dht.save(file)
            self.show_status(f"File saved successfully to {file}.")
        except Exception as e:
            self.show_error(str(e))

    def view_contact(self):
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
            appearance_mode=self.appearance_mode
        )
        contact_viewer.mainloop()


class ErrorWindow(ctk.CTk):
    def __init__(self, error_message: str):
        super().__init__()
        self.title = ctk.CTkLabel(self, text="Error", font=Fonts.title_font)
        self.title.pack(padx=20, pady=20)

        self.error_message = ctk.CTkLabel(self, text=error_message, font=Fonts.text_font)
        self.error_message.pack(padx=20, pady=10)


class MainGUI(ctk.CTk):
    def __init__(self, appearance_mode="dark"):
        ctk.CTk.__init__(self)
        self.settings_button = None
        self.appearance_mode = appearance_mode
        ctk.set_appearance_mode(appearance_mode)
        # self.geometry("600x500")
        self.title("Kademlia")

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
        logger.info("Initialising Kademlia.")

        our_id = id.ID.random_id()
        if USE_GLOBAL_IP:  # Port forwarding is required.
            our_ip = get('https://api.ipify.org').content.decode('utf8')
        else:
            our_ip = "127.0.0.1"
        logger.info(f"Our hostname is {our_ip}.")

        if PORT:
            valid_port = PORT
        else:
            valid_port = helpers.get_valid_port()

        logger.info(f"Port free at {valid_port}, creating our node here.")

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
        logger.info(f"Making directory at {create_dir_at}")
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
            settings_window = Settings(hash_table=self.dht, appearance_mode=self.appearance_mode)
            settings_window.mainloop()
        else:
            pass

    def thread_open_settings(self):
        """
        OBSELETE - open_settings works fine.
        Opens the server window in a thread - this is not recommended to use.
        :return:
        """
        settings_thread = threading.Thread(target=self.open_settings, daemon=True)
        settings_thread.daemon = True  # Dies when program ends.
        settings_thread.start()

    def add_settings_icon(self):
        dark_icon = Image.open(r"assets/settings_icon_light.png")
        light_icon = Image.open(r"assets/settings_icon_dark.png")
        settings_icon = ctk.CTkImage(light_image=light_icon, dark_image=dark_icon, size=(30, 30))
        self.settings_button = ctk.CTkButton(self, image=settings_icon, text="",
                                             bg_color="transparent", fg_color="transparent",
                                             width=28, command=self.open_settings)

        self.settings_button.pack(side=ctk.BOTTOM, anchor=ctk.S, padx=10, pady=10)

    def clear_screen(self):
        for child in self.winfo_children():
            child.destroy()

    def clear_screen_and_keep_settings(self):
        self.clear_screen()
        if hasattr(self, "dht"):
            self.add_settings_icon()

    def make_join_dht_frame(self):
        self.clear_screen_and_keep_settings()
        join = JoinNetworkMenuFrame(parent=self)
        join.pack(padx=20, pady=20)

    def make_load_dht_frame(self):
        self.clear_screen_and_keep_settings()
        load_dht = LoadDHTFromFileFrame(parent=self)
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
        logger.error(error_message)
        error_window = ErrorWindow(error_message)
        error_window.mainloop()

    @classmethod
    def show_status(cls, message: str, copy_data=None):
        logger.info(message)
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


class UploadFrame(ctk.CTkFrame):
    def __init__(self, parent: MainGUI, fg_color="transparent", **kwargs):
        ctk.CTkFrame.__init__(self, parent, **kwargs)
        self.configure(fg_color=fg_color)
        self.parent = parent

        self.title = ctk.CTkLabel(self, text="Upload File", font=Fonts.title_font)
        self.title.grid(column=0, row=0, columnspan=2, padx=20, pady=10)

        self.enter_file_label = ctk.CTkLabel(self, text="File to upload:", font=Fonts.text_font)
        self.enter_file_label.grid(column=0, row=1, padx=20, pady=10)

        self.enter_file_entry = ctk.CTkEntry(self, width=150, height=20, font=Fonts.text_font)
        self.enter_file_entry.grid(column=1, row=1, padx=20, pady=10)

        self.back_button = ctk.CTkButton(self, text="Back", font=Fonts.text_font,
                                         command=self.parent.make_network_frame)
        self.back_button.grid(column=0, row=2, padx=20, pady=10)

        self.upload_button = ctk.CTkButton(self, text="Upload", font=Fonts.text_font,
                                           command=self.handle_upload)
        self.upload_button.grid(column=1, row=2, columnspan=1, padx=20, pady=10)

    def handle_upload(self):
        """
        Stores a file on our system on the network.
        This is stored by storing key: Random ID, value: Pickled dictionary
        {"filename": filename, "file": file_contents}
        The pickled dictionary is decoded using Constants.PICKLE_ENCODING (default: "latin1"), because the
        value sent must be a string.
        """
        file_to_upload = self.enter_file_entry.get().strip("\n")
        if isfile(file_to_upload):
            filename = os.path.basename(file_to_upload)
            if not filename:  # os.path.basename returns "" on file paths ending in "/"
                logger.error("File to upload must not be a directory.")
                self.parent.show_error("Must not be a directory.")
            else:
                id_to_store_to = helpers.store_file(file_to_upload, self.parent.dht)
                self.parent.show_status(f"Stored file at {id_to_store_to.value}.", copy_data=str(id_to_store_to.value))
        else:
            logger.error(f"Path not found: {file_to_upload}")
            self.parent.show_error(f"Path not found: {file_to_upload}")


class DownloadFrame(ctk.CTkFrame):
    def __init__(self, parent: MainGUI, fg_color="transparent", **kwargs):
        ctk.CTkFrame.__init__(self, parent, **kwargs)
        self.configure(fg_color=fg_color)
        self.parent = parent

        self.title = ctk.CTkLabel(self, text="Download File", font=Fonts.title_font)
        self.title.grid(column=0, row=0, columnspan=2, padx=20, pady=10)

        self.enter_id_label = ctk.CTkLabel(self, text="ID to download:", font=Fonts.text_font)
        self.enter_id_label.grid(column=0, row=1, padx=20, pady=10)

        self.enter_id_entry = ctk.CTkEntry(self, width=150, height=20, font=Fonts.text_font)
        self.enter_id_entry.grid(column=1, row=1, padx=20, pady=10)

        self.back_button = ctk.CTkButton(self, text="Back", font=Fonts.text_font,
                                         command=self.parent.make_network_frame)
        self.back_button.grid(column=0, row=2, padx=20, pady=10)

        self.download_button = ctk.CTkButton(self, text="Download", font=Fonts.text_font,
                                             command=self.handle_download)
        self.download_button.grid(column=1, row=2, columnspan=1, padx=20, pady=10)

    def handle_download(self):
        id_from_entry: str = self.enter_id_entry.get().strip("\n")

        if not id_from_entry:
            self.parent.show_error("ID must not be empty.")
        elif not id_from_entry.isnumeric():
            self.parent.show_error("ID was not a number.")
        elif not 0 <= int(id_from_entry) < 2 ** Constants.ID_LENGTH_BITS:
            self.parent.show_error("ID out of range.")
        else:
            id_to_download: id.ID = id.ID(int(id_from_entry))
            found, contacts, val = self.parent.dht.find_value(key=id_to_download)
            # val will be a 'latin1' pickled dictionary {filename: str, file: bytes}
            if not found:
                self.parent.show_error("File not found.")
            else:
                # TODO: It might be a better idea to use JSON to send values.

                val_bytes: bytes = val.encode(Constants.PICKLE_ENCODING)

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

                self.parent.show_status(f"File downloaded to {os.path.join(cwd, filename)}.")


class MainNetworkFrame(ctk.CTkFrame):
    def __init__(self, parent: MainGUI, fg_color="transparent", **kwargs):
        ctk.CTkFrame.__init__(self, parent, **kwargs)
        self.configure(fg_color=fg_color)
        self.parent = parent

        self.title = ctk.CTkLabel(self, text="Kademlia", font=Fonts.title_font)
        self.title.grid(column=0, row=0, columnspan=2, padx=20, pady=10)

        self.download_button = ctk.CTkButton(self, text="Download", font=Fonts.text_font,
                                             command=self.parent.make_download_frame)
        self.download_button.grid(column=0, row=1, padx=10, pady=10)

        self.upload_button = ctk.CTkButton(self, text="Upload", font=Fonts.text_font,
                                           command=self.parent.make_upload_frame)
        self.upload_button.grid(column=1, row=1, padx=10, pady=10)


class LoadDHTFromFileFrame(ctk.CTkFrame):
    def __init__(self, parent: MainGUI, fg_color="transparent", **kwargs):
        ctk.CTkFrame.__init__(self, parent, **kwargs)
        self.configure(fg_color=fg_color)
        self.parent = parent

        self.load_title = ctk.CTkLabel(master=self, text="Load DHT from file", font=Fonts.title_font)
        self.load_title.grid(column=0, row=0, columnspan=2, padx=20, pady=10)

        self.enter_file_name_text = ctk.CTkLabel(master=self, text="Load from file: ", font=Fonts.text_font)
        self.enter_file_name_text.grid(column=0, row=1, padx=20, pady=20)

        self.file_name_entry = ctk.CTkEntry(master=self, width=150, height=30, font=Fonts.text_font)
        self.file_name_entry.grid(column=1, row=1, padx=20, pady=20)

        self.back_button = ctk.CTkButton(master=self, text="Back", font=Fonts.text_font,
                                         command=self.parent.make_join_dht_frame)
        self.back_button.grid(column=0, row=2, padx=20, pady=0)

        self.load_button = ctk.CTkButton(master=self, text="Load DHT", font=Fonts.text_font,
                                         command=self.load_dht)
        self.load_button.grid(column=1, row=2, padx=20, pady=0)

    def load_dht(self):
        filename = self.file_name_entry.get().strip("\n")

        if not isfile(filename):
            self.parent.show_error(f"File not found:\n'{filename}'")
            return
        try:
            loaded_dht = dht.DHT.load(filename=filename)
        except pickle.UnpicklingError:
            self.parent.show_error("File was invalid.")
            return

        try:
            self.parent.server.thread_stop(self.parent.server_thread)
        except:
            # The server hasn't been set up, any error doesn't matter
            # because a new ones being made anyway.
            pass

        self.parent.dht = loaded_dht

        try:
            self.parent.server = networking.TCPServer(self.parent.dht.node)
            self.parent.server_thread = self.parent.server.thread_start()
            self.parent.make_network_frame()
        except Exception as e:
            self.parent.show_error(str(e))
            return


class JoinNetworkMenuFrame(ctk.CTkFrame):
    """
      └── Join
          ├── Settings
          ├── Load an existing network
          ├── Create new network
          └── Bootstrap into a new network
    """

    def __init__(self, parent: MainGUI, fg_color="transparent", **kwargs):
        ctk.CTkFrame.__init__(self, parent, **kwargs)

        self.configure(fg_color=fg_color)
        self.parent = parent

        join_title = ctk.CTkLabel(master=self, text="Join Network", font=Fonts.title_font)
        join_title.pack(padx=50, pady=20)

        self.load_button = ctk.CTkButton(master=self, text="Join stored network", font=Fonts.text_font,
                                         command=self.parent.make_load_dht_frame)
        self.load_button.pack(padx=50, pady=10)

        self.bootstrap_button = ctk.CTkButton(master=self, text="Bootstrap into existing network", font=Fonts.text_font,
                                              command=self.parent.make_bootstrap_frame)
        self.bootstrap_button.pack(padx=50, pady=10)

        self.create_new_network_button = ctk.CTkButton(master=self, text="Create new network", font=Fonts.text_font,
                                                       command=self.parent.initialise_kademlia)
        self.create_new_network_button.pack(padx=50, pady=10)


class BootstrapFromJSONFrame(ctk.CTkFrame):
    def __init__(self, parent: MainGUI, fg_color="transparent", **kwargs):
        ctk.CTkFrame.__init__(self, parent, **kwargs)
        self.configure(fg_color=fg_color)
        self.parent = parent

        self.title = ctk.CTkLabel(self, text="Bootstrap from Contact JSON", font=Fonts.title_font)
        self.title.grid(row=0, column=0, columnspan=2, padx=10, pady=20)

        filename_label = ctk.CTkLabel(master=self, text="Filename:", font=Fonts.text_font)
        filename_label.grid(row=1, column=0, padx=5, pady=5, sticky="nsew")

        self.filename_entry = ctk.CTkEntry(master=self, width=150, font=Fonts.text_font)
        self.filename_entry.grid(row=1, column=1, padx=5, pady=5, sticky="ew")

        self.back_button = ctk.CTkButton(master=self, text="Back", font=Fonts.text_font,
                                         command=self.parent.make_join_dht_frame)
        self.back_button.grid(row=2, column=0, columnspan=1, padx=10, pady=10)

        self.load_button = ctk.CTkButton(master=self, text="Load", font=Fonts.text_font,
                                         command=self.load_known_peer_json_for_bootstrap)
        self.load_button.grid(row=2, column=1, columnspan=1, padx=10, pady=10)

    def load_known_peer_json_for_bootstrap(self):
        filename = self.filename_entry.get().strip("\n")
        if not exists(filename):
            self.parent.show_error("Couldn't find file to bootstrap from.")
        else:
            with open(filename, "r") as f:
                contact_dict = json.load(f)

            known_id = None
            if "id" in contact_dict:
                known_id = contact_dict["id"]
            else:
                self.parent.show_error("File to bootstrap from had no \nparameter 'id'.")

            known_url = None
            if "url" in contact_dict:
                known_url = contact_dict["url"]
            else:
                self.parent.show_error("File to bootstrap from had no \nparameter 'url'.")

            known_port = None
            if "port" in contact_dict:
                known_port = contact_dict["port"]
            else:
                self.parent.show_error("File to bootstrap from had no \nparameter 'port'.")

            if known_url and known_port and known_url:
                success_bootstrapping: bool = BootstrapFrame.bootstrap(
                    parent=self.parent,
                    known_id=id.ID(known_id),
                    known_url=known_url,
                    known_port=known_port
                )
                if success_bootstrapping:
                    self.parent.make_network_frame()


class BootstrapFrame(ctk.CTkFrame):
    def __init__(self, parent: MainGUI, fg_color="transparent", **kwargs):

        ctk.CTkFrame.__init__(self, parent, **kwargs)
        self.configure(fg_color=fg_color)
        self.parent = parent

        self.title = ctk.CTkLabel(self, text="Bootstrap from known peer", font=Fonts.title_font)
        self.title.grid(row=0, column=0, columnspan=2, padx=10, pady=20)

        ip_text = ctk.CTkLabel(master=self, text="IP Address: ")
        ip_text.grid(row=1, column=0, padx=5, pady=10, sticky="nsew")

        self.ip_entry = ctk.CTkEntry(master=self, width=150)
        self.ip_entry.grid(row=1, column=1, padx=5, pady=10, sticky="ew")

        port_text = ctk.CTkLabel(master=self, text="Port: ")
        port_text.grid(row=2, column=0, padx=5, pady=10, sticky="nsew")

        self.port_entry = ctk.CTkEntry(master=self, width=150)
        self.port_entry.grid(row=2, column=1, padx=5, pady=10, sticky="ew")

        id_text = ctk.CTkLabel(master=self, text="ID: ")
        id_text.grid(row=3, column=0, padx=5, pady=10, sticky="nsew")

        self.id_entry = ctk.CTkEntry(master=self, width=150)
        self.id_entry.grid(row=3, column=1, padx=5, pady=10, sticky="ew")

        self.back_button = ctk.CTkButton(self, text="Back", font=Fonts.text_font,
                                         command=self.parent.make_join_dht_frame)
        self.back_button.grid(row=4, column=0, columnspan=1, padx=5, pady=10)

        self.load_from_json_button = ctk.CTkButton(self, text="Load from file", font=Fonts.text_font,
                                                   command=self.parent.make_bootstrap_from_json_frame)
        self.load_from_json_button.grid(row=4, column=1, columnspan=1, padx=5, pady=10)

        self.connect_button = ctk.CTkButton(master=self, text="Connect", font=Fonts.text_font,
                                            command=self.handle_bootstrap)
        self.connect_button.grid(row=5, column=0, columnspan=2, padx=5, pady=10)

    def handle_bootstrap(self):
        valid = False

        known_ip: str = self.ip_entry.get().strip("\n")
        ip_regex = r"(([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])\.){3}([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])"
        if not known_ip:
            self.parent.show_error("IP address must not be empty.")
        elif not re.match(string=known_ip, pattern=ip_regex):
            self.parent.show_error("IP address is invalid.")
        else:
            valid = True

        known_port_str: str = self.port_entry.get().strip("\n")
        known_port = None
        if not known_port_str:
            self.parent.show_error("Port must not be empty.")
        elif not known_port_str.isnumeric():
            self.parent.show_error("Port was not a number.")
        elif int(known_port_str) < 0 or int(known_port_str) > 65535:
            self.parent.show_error("Port was out of range. Must be between 0 and 65535.")
        else:
            known_port = int(known_port_str)
            valid = True

        known_id_value: str = self.id_entry.get().strip("\n")
        known_id = None
        if not known_id_value:
            self.parent.show_error("ID must not be empty.")
        elif not known_id_value.isnumeric():
            self.parent.show_error("ID was not a number.")
        elif int(known_id_value) < 0 or int(known_id_value) >= 2 ** Constants.ID_LENGTH_BITS:
            # what if they want to change ID range?
            self.parent.show_error("ID out of range")
        else:
            known_id = id.ID(int(known_id_value))
            valid = True

        if known_id and known_ip and known_port and valid:
            self.bootstrap(self.parent, known_id, known_ip, known_port)

    @classmethod
    def bootstrap(cls, parent: MainGUI, known_id: id.ID, known_url: str, known_port: int) -> bool:
        """Attempts to bootstrap Kademlia connection from a known contact"""
        known_protocol = protocols.TCPProtocol(
            url=known_url, port=known_port
        )

        known_contact: contact.Contact = contact.Contact(
            id=known_id,
            protocol=known_protocol
        )
        logger.debug("Bootstrapping from known contact")
        if not hasattr(parent, "dht"):
            parent.initialise_kademlia()
            return True

        logger.info("Attempting to connect to known peer's network...")
        try:
            parent.dht.bootstrap(known_contact)
            logger.info("Connected to known peer's network.")
            return True

        except errors.RPCError as e:
            if e.timeout_error:
                parent.show_error("Timeout error trying to contact known peer.")
                return False
            elif e.id_mismatch_error:
                parent.show_error("Random ID returned does not match what was sent.")
                return False
            elif e.peer_error:
                parent.show_error(f"Peer error: {e}")
                return False
            elif e.protocol_error:
                parent.show_error(f"Protocol error: {e}")
                return False
        except Exception as e:
            parent.show_error(str(e))
            return False


if __name__ == "__main__":
    app = MainGUI("dark")  # can also be light
    app.mainloop()
    logger.info("Done!")
    exit(0)
