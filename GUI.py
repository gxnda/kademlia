import threading

import customtkinter as ctk
from PIL import Image
from requests import get

from kademlia import dht, id, networking, protocols, node, contact, storage, routers

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


class Fonts:
    title_font = ("Segoe UI", 20, "bold")
    text_font = ("Segoe UI", 16)


class ContactViewer(ctk.CTk):
    def __init__(self, id: int, protocol_type: type, url: str, port: int, appearance_mode="dark"):
        super().__init__()
        ctk.set_appearance_mode(appearance_mode)

        self.settings_title = ctk.CTkLabel(self, text="Our Contact:", font=Fonts.title_font)
        self.settings_title.pack(padx=20, pady=30)

        self.id = ctk.CTkLabel(self, text=f"ID: {id}", font=Fonts.text_font)
        self.id.pack(padx=20, pady=10)

        self.protocol_type = ctk.CTkLabel(self, text=f"Protocol type: {protocol_type}", font=Fonts.text_font)
        self.protocol_type.pack(padx=20, pady=10)

        self.url = ctk.CTkLabel(self, text=f"URL: {url}", font=Fonts.text_font)
        self.url.pack(padx=20, pady=10)

        self.port = ctk.CTkLabel(self, text=f"Port: {port}", font=Fonts.text_font)
        self.port.pack(padx=20, pady=10)


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

        self.export_dht_button = ctk.CTkButton(self, text="Export DHT", font=Fonts.text_font, command=self.export_dht)
        self.export_dht_button.grid(column=1, row=2, padx=10, pady=10)

        self.view_contact_button = ctk.CTkButton(self, text="View our contact", font=Fonts.text_font,
                                                 command=self.view_contact)
        self.view_contact_button.grid(column=0, row=2, padx=10, pady=10)

    def export_dht(self):
        file = self.dht_export_file.get("0.0", "end").strip("\n")
        self.dht.save(file)

    def view_contact(self):
        our_contact: contact.Contact = self.dht.our_contact
        our_id: int = our_contact.id.value
        # noinspection PyTypeChecker
        protocol: protocols.TCPSubnetProtocol = our_contact.protocol
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
        self.initialise_kademlia()

        self.make_join_dht_frame()

    def initialise_kademlia(self):
        """
        Initialises Kademlia protocol.
        :return:
        """
        # TODO: This still uses kademlia.networking.TCPSubnetProtocol,
        #     kademlia.networking.TCPProtocol will be ideal in the future.
        #     (Should still work for external references, just a bit iffy)
        print("[Initialisation] Initialising Kademlia.")

        our_id = id.ID.random_id()
        our_ip = get('https://api.ipify.org').content.decode('utf8')
        print(f"[Initialisation] Our hostname is {our_ip}.")

        valid_port = None
        for port in range(10000, 30000):
            if networking.port_is_free(port):
                valid_port = port
                break
        if not valid_port:
            raise OSError("No ports free!")

        print(f"[Initialisation] Port free at {valid_port}, creating our node here.")

        protocol = protocols.TCPSubnetProtocol(
            url=our_ip, port=valid_port, subnet=1
        )

        our_node = node.Node(
            contact=contact.Contact(
                id=our_id,
                protocol=protocol
            ),
            storage=storage.SecondaryStorage(f"{our_id.value}/node.json"),
            cache_storage=storage.VirtualStorage()
        )

        self.dht: dht.DHT = dht.DHT(
            id=our_id,
            protocol=protocol,
            originator_storage=storage.SecondaryStorage(f"{our_id.value}/originator_storage.json"),
            republish_storage=storage.SecondaryStorage(f"{our_id.value}/republish_storage.json"),
            cache_storage=storage.VirtualStorage(),
            router=routers.ParallelRouter(our_node)
        )

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
        self.add_settings_icon()

    def make_join_dht_frame(self):
        self.clear_screen()
        join = JoinFrame(parent=self)
        join.pack(padx=20, pady=20)

    def make_load_dht_frame(self):
        self.clear_screen()
        load_dht = LoadDHTFrame(parent=self)
        load_dht.pack(padx=20, pady=20)

    def make_bootstrap_frame(self):
        self.clear_screen()
        bootstrap = BootstrapFrame(parent=self)
        bootstrap.pack(padx=20, pady=20)

    def make_network_page(self):
        self.clear_screen()
        # TODO: Create.

    def show_error(self, error_message: str):
        print(f"[Error] {error_message}")
        error_window = ErrorWindow(error_message)
        error_window.mainloop()


class LoadDHTFrame(ctk.CTkFrame):
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
        loaded_dht = dht.DHT.load(filename=filename)
        self.parent.dht = loaded_dht

        self.parent.make_network_page()


class JoinFrame(ctk.CTkFrame):
    """
      └── Join
          ├── Settings
          ├── Load an existing network
          └── Bootstrap into a new network
    """
    def __init__(self, parent: MainGUI, fg_color="transparent", **kwargs):
        ctk.CTkFrame.__init__(self, parent, **kwargs)

        self.configure(fg_color=fg_color)
        self.parent = parent

        join_title = ctk.CTkLabel(master=self, text="Join a Network", font=Fonts.title_font)
        join_title.pack(padx=50, pady=20)

        self.load_button = ctk.CTkButton(master=self, text="Load an existing network", font=Fonts.text_font,
                                         command=self.parent.make_load_dht_frame)
        self.load_button.pack(padx=50, pady=10)

        self.bootstrap_button = ctk.CTkButton(master=self, text="Join a new network", font=Fonts.text_font,
                                              command=self.parent.make_bootstrap_frame)
        self.bootstrap_button.pack(padx=50, pady=10)

    def clear_screen(self):
        for child in self.winfo_children():
            child.destroy()


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

        self.connect_button = ctk.CTkButton(master=self, text="Connect", font=Fonts.text_font,
                                            command=self.handle_bootstrap)
        self.connect_button.grid(row=4, column=1, columnspan=1, padx=5, pady=10)

    def handle_bootstrap(self):
        valid = False

        known_ip: str = self.ip_entry.get().strip("\n")
        if not known_ip:
            self.parent.show_error("IP address must not be empty.")
        else:
            valid = True

        known_port_str: str = self.port_entry.get().strip("\n")
        if not known_port_str:
            self.parent.show_error("Port must not be empty.")
        elif not known_port_str.isnumeric():
            self.parent.show_error("Port was not a number.")
        else:
            known_port: int = int(known_port_str)
            valid = True

        known_id_value: str = self.id_entry.get().strip("\n")
        if not known_id_value:
            self.parent.show_error("ID must not be empty.")
        elif not known_id_value.isnumeric():
            self.parent.show_error("ID was not a number.")

        else:
            known_id: id.ID = id.ID(int(known_id_value))
            valid = True

        if valid:
            self.bootstrap(known_id, known_ip, known_port)

    def bootstrap(self, known_id: id.ID, known_url: str, known_port: int):
        """Attempts to bootstrap Kademlia connection from a known contact"""
        known_protocol = protocols.TCPSubnetProtocol(
            url=known_url, port=known_port, subnet=1  # TODO: Replace with TCPProtocol
        )

        known_contact: contact.Contact = contact.Contact(
            id=known_id,
            protocol=known_protocol
        )
        print("[GUI] Bootstrapping from known contact")
        self.parent.dht.bootstrap(known_contact)

        print("[GUI] Connecting to bootstrap server...")


if __name__ == "__main__":
    app = MainGUI("light")
    app.mainloop()
    print("Done!")
