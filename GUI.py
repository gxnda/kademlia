import os
import pickle
import re
import threading
import json
from os.path import exists, isfile
from random import randint

import customtkinter as ctk
from PIL import Image
from requests import get

from kademlia import dht, id, networking, protocols, node, contact, storage, routers
from kademlia.constants import Constants

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
USE_GLOBAL_IP = False


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

        self.settings_title = ctk.CTkLabel(self, text="Our Contact:", font=Fonts.title_font)
        self.settings_title.pack(padx=20, pady=30)

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
        print(f"[Error] {error_message}")
        error_window = ErrorWindow(error_message)
        error_window.mainloop()

    def export_contact(self, filename="our_contact.json"):
        contact_dict = {
            "url": self.url,
            "port": self.port,
            "protocol_type": str(self.protocol_type),
            "id": self.id
        }
        print("[Status] Exporting our contact...")
        with open(filename, "w") as f:
            json.dump(contact_dict, f)
        self.show_status(f"Exported our contact to {filename}.")

    def show_status(self, message: str):
        print(f"[Status] {message}")
        status_window = StatusWindow(message)
        status_window.mainloop()


class StatusWindow(ctk.CTk):
    def __init__(self, message: str, copy_data=None):
        ctk.CTk.__init__(self)
        self.copy_data = copy_data
        self.message = ctk.CTkLabel(self, text=message, font=Fonts.text_font)
        self.message.pack(padx=30, pady=20)
        if copy_data:
            self.copy_button = ctk.CTkButton = ctk.CTkButton(self, text="Copy to clipboard", font=Fonts.text_font,
                                                             command=self.copy)
            self.copy_button.pack(padx=30, pady=20)

    def copy(self):
        print(f"[GUI] Copying data to clipboard: {self.copy_data}")
        self.clipboard_clear()
        self.clipboard_append(self.copy_data)
        self.update()


class Settings(ctk.CTk):
    def __init__(self, hash_table: dht.DHT, appearance_mode="dark"):
        super().__init__()
        ctk.set_appearance_mode(appearance_mode)

        self.appearance_mode = appearance_mode

        self.dht: dht.DHT = hash_table

        self.title("Kademlia Settings")

        self.settings_title = ctk.CTkLabel(self, text="Settings", font=Fonts.title_font)
        self.settings_title.grid(column=0, row=0, columnspan=2, padx=20, pady=20)

        self.dht_export_label = ctk.CTkLabel(self, text="File to export to:", width=150, font=Fonts.text_font)
        self.dht_export_label.grid(column=0, row=1, padx=10, pady=10)

        self.dht_export_file = ctk.CTkTextbox(self, width=200, height=20, font=Fonts.text_font)
        self.dht_export_file.grid(column=1, row=1, padx=10, pady=10)
        self.dht_export_file.insert("1.0", f"dht.pickle")

        self.export_dht_button = ctk.CTkButton(self, text="Export/Save DHT", font=Fonts.text_font, command=self.export_dht)
        self.export_dht_button.grid(column=1, row=2, padx=10, pady=10)

        self.view_contact_button = ctk.CTkButton(self, text="View our contact", font=Fonts.text_font,
                                                 command=self.view_contact)
        self.view_contact_button.grid(column=0, row=2, padx=10, pady=10)

    def show_error(self, error_message: str):
        print(f"[Error] {error_message}")
        error_window = ErrorWindow(error_message)
        error_window.mainloop()

    def show_status(self, message: str):
        print(f"[Status] {message}")
        status_window = StatusWindow(message)
        status_window.mainloop()

    def export_dht(self):
        file = self.dht_export_file.get("0.0", "end").strip("\n")
        try:
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
        self.appearance_mode = appearance_mode
        ctk.set_appearance_mode(appearance_mode)
        # self.geometry("600x500")
        self.title("Kademlia")

        # Create our contact - this should be overwritten if bootstrapping.
        # self.initialise_kademlia()

        self.make_join_dht_frame()

    def initialise_kademlia(self):
        """
        Initialises Kademlia protocol.
        :return:
        """
        print("[Initialisation] Initialising Kademlia.")

        our_id = id.ID.random_id()
        if USE_GLOBAL_IP:  # Port forwarding is required.
            our_ip = get('https://api.ipify.org').content.decode('utf8')
        else:
            our_ip = "127.0.0.1"
        print(f"[Initialisation] Our hostname is {our_ip}.")

        valid_port = None
        while not valid_port:  # TODO: This will be stuck in an infinite loop if all ports are full.
            port = randint(5000, 35000)
            if networking.port_is_free(port):
                valid_port = port

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
        print(create_dir_at)
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
        settings_window = Settings(hash_table=self.dht, appearance_mode=self.appearance_mode)
        settings_window.mainloop()

    def thread_open_settings(self):
        """
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
        self.add_settings_icon()

    def make_join_dht_frame(self):
        self.clear_screen()
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
        bootstrap_from_json = BootstrapFromJSON(parent=self)
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
        self.clear_screen()
        download_frame = DownloadFrame(self)
        download_frame.pack(padx=20, pady=20)

    def make_upload_frame(self):
        self.clear_screen()
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

        self.enter_file_textbox = ctk.CTkTextbox(self, width=150, height=20, font=Fonts.text_font)
        self.enter_file_textbox.grid(column=1, row=1, padx=20, pady=10)

        self.back_button = ctk.CTkButton(self, text="Back", font=Fonts.text_font,
                                         command=self.parent.make_network_frame)
        self.back_button.grid(column=0, row=2, padx=20, pady=10)

        self.upload_button = ctk.CTkButton(self, text="Upload", font=Fonts.text_font,
                                           command=self.handle_upload)
        self.upload_button.grid(column=1, row=2, columnspan=1, padx=20, pady=10)

    def handle_upload(self):
        file_to_upload = self.enter_file_textbox.get("0.0", "end").strip("\n")
        if isfile(file_to_upload):
            filename = os.path.basename(file_to_upload)
            if not filename:  # os.path.basename returns "" on file paths ending in "/"
                self.parent.show_error("Must not be a directory.")
            else:
                with open(file_to_upload, "rb") as f:
                    file_contents: bytes = f.read()
                # val will be a 'latin1' pickled dictionary {filename: str, file: bytes}
                val: str = pickle.dumps({"filename": filename, "file": file_contents}).decode("latin1")
                id_to_store_to = id.ID.random_id()
                self.parent.dht.store(id_to_store_to, val)
                self.parent.show_status(f"Stored file at {id_to_store_to}.", copy_data=str(id_to_store_to))
        else:
            self.parent.show_error(f"Path not found: {file_to_upload}")


class DownloadFrame(ctk.CTkFrame):
    def __init__(self, parent: MainGUI, fg_color="transparent", **kwargs):
        ctk.CTkFrame.__init__(self, parent, **kwargs)
        self.configure(fg_color=fg_color)
        self.parent = parent

        self.title = ctk.CTkLabel(self, text="Download File", font=Fonts.title_font)
        self.title.grid(column=0, row=0, columnspan=2, padx=20, pady=10)

        self.enter_file_label = ctk.CTkLabel(self, text="ID to download:", font=Fonts.text_font)
        self.enter_file_label.grid(column=0, row=1, padx=20, pady=10)

        self.enter_id_textbox = ctk.CTkTextbox(self, width=150, height=20, font=Fonts.text_font)
        self.enter_id_textbox.grid(column=1, row=1, padx=20, pady=10)

        self.back_button = ctk.CTkButton(self, text="Back", font=Fonts.text_font,
                                         command=self.parent.make_network_frame)
        self.back_button.grid(column=0, row=2, padx=20, pady=10)

        self.download_button = ctk.CTkButton(self, text="Download", font=Fonts.text_font,
                                             command=self.handle_download)
        self.download_button.grid(column=1, row=2, columnspan=1, padx=20, pady=10)

    def handle_download(self):
        id_from_textbox: str = self.enter_id_textbox.get("0.0", "end").strip("\n")

        if not id_from_textbox:
            self.parent.show_error("ID must not be empty.")
        elif not id_from_textbox.isnumeric():
            self.parent.show_error("ID was not a number.")
        elif not 0 < int(id_from_textbox) < 2 ** Constants.ID_LENGTH_BITS:
            self.parent.show_error("ID out of range.")
        else:
            id_to_download: id.ID = id.ID(int(id_from_textbox))
            found, contacts, val = self.parent.dht.find_value(key=id_to_download)
            # val will be a 'latin1' pickled dictionary {filename: str, file: bytes}
            if not found:
                self.parent.show_error("File not found.")
            else:
                val_bytes: bytes = val.encode("latin1")  # TODO: Add option for changing this in settings.
                file_dict: dict = pickle.loads(val_bytes)
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

        self.file_name_textbox = ctk.CTkTextbox(master=self, width=150, height=30)
        self.file_name_textbox.grid(column=1, row=1, padx=20, pady=20)

        self.back_button = ctk.CTkButton(master=self, text="Back", font=Fonts.text_font,
                                         command=self.parent.make_join_dht_frame)
        self.back_button.grid(column=0, row=2, padx=20, pady=0)

        self.load_button = ctk.CTkButton(master=self, text="Load DHT", font=Fonts.text_font,
                                         command=self.load_dht)
        self.load_button.grid(column=1, row=2, padx=20, pady=0)

    def load_dht(self):
        filename = self.file_name_textbox.get("0.0", "end").strip("\n")

        if not isfile(filename):
            self.parent.show_error(f"File not found:\n'{filename}'")
            return None

        loaded_dht = dht.DHT.load(filename=filename)
        self.parent.server.thread_stop(self.parent.server_thread)
        self.parent.dht = loaded_dht

        try:
            self.parent.server = networking.TCPServer(self.parent.dht.node)
            self.parent.server_thread = self.parent.server.thread_start()
        except Exception as e:
            self.parent.show_error(str(e))

        self.parent.make_network_frame()


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


class BootstrapFromJSON(ctk.CTkFrame):
    def __init__(self, parent: MainGUI, **kwargs):
        ctk.CTkFrame.__init__(self, parent, **kwargs)
        self.configure(fg_color="transparent")
        self.parent = parent

        self.title = ctk.CTkLabel(self, text="Bootstrap from JSON", font=Fonts.title_font)
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
                BootstrapFrame.bootstrap(
                    parent=self.parent,
                    known_id=id.ID(known_id),
                    known_url=known_url,
                    known_port=known_port
                )


class BootstrapFrame(ctk.CTkFrame):
    def __init__(self, parent: MainGUI, **kwargs):
        
        ctk.CTkFrame.__init__(self, parent, **kwargs)
        self.configure(fg_color="transparent")
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

        self.return_to_menu_button = ctk.CTkButton(self, text="Back", font=Fonts.text_font,
                                                   command=self.parent.make_join_dht_frame)
        self.return_to_menu_button.grid(row=4, column=0, columnspan=1, padx=5, pady=10)

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
        elif not re.match(known_ip, ip_regex):
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
        elif int(known_id_value) < 0 or int(known_id_value) > 2 ** Constants.ID_LENGTH_BITS:
            # what if they want to change ID range?
            self.parent.show_error("ID out of range")
        else:
            known_id = id.ID(int(known_id_value))
            valid = True

        if known_id and known_ip and known_port and valid:
            self.bootstrap(self.parent, known_id, known_ip, known_port)

    @classmethod
    def bootstrap(cls, parent: MainGUI, known_id: id.ID, known_url: str, known_port: int):
        """Attempts to bootstrap Kademlia connection from a known contact"""
        known_protocol = protocols.TCPProtocol(
            url=known_url, port=known_port
        )

        known_contact: contact.Contact = contact.Contact(
            id=known_id,
            protocol=known_protocol
        )
        print("[GUI] Bootstrapping from known contact")
        parent.dht.bootstrap(known_contact)

        print("[GUI] Connecting to bootstrap server...")


if __name__ == "__main__":
    app = MainGUI("light")
    app.mainloop()
    print("Done!")
