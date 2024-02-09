import threading
from socket import gethostname

import customtkinter as ctk
from PIL import Image

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


class KademliaFonts:
    pass


class Settings(ctk.CTk):
    def __init__(self, hash_table: dht.DHT, appearance_mode="dark"):
        super().__init__()
        ctk.set_appearance_mode(appearance_mode)

        self.dht: dht.DHT = hash_table

        self.geometry("200x200")
        self.title("Kademlia Settings")

        self.settings_title = ctk.CTkLabel(self, text="Settings", font=("Aharoni", 20, "bold"))
        self.settings_title.pack(padx=20, pady=30)

        self.export_dht_button = ctk.CTkButton(self, text="Export", command=self.export_dht)
        self.export_dht_button.pack(padx=20, pady=10)

        self.view_contact_button = ctk.CTkButton(self, text="View our contact", command=self.view_contact)
        self.view_contact_button.pack(padx=20, pady=10)

    def export_dht(self):
        self.dht.save
        pass

    def view_contact(self):
        # TODO: Create
        pass


class MainGUI(ctk.CTk):
    def __init__(self, appearance_mode="dark"):
        ctk.CTk.__init__(self)
        self.appearance_mode = appearance_mode
        self.geometry("600x500")
        self.title("Kademlia")

        self.initialise_kademlia()

        print(self.dht.__dict__)
        ctk.set_appearance_mode(appearance_mode)
        dark_icon = Image.open(r"assets/settings_icon_light.png")
        light_icon = Image.open(r"assets/settings_icon_dark.png")
        settings_icon = ctk.CTkImage(light_image=light_icon, dark_image=dark_icon, size=(30, 30))
        self.settings_button = ctk.CTkButton(self, image=settings_icon, text="",
                                             bg_color="transparent", fg_color="transparent",
                                             width=28, command=self.open_settings)
        self.load_join_window()
        self.add_settings_icon()

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
        our_ip = gethostname()
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
        settings_thread = threading.Thread(target=self.open_settings, daemon=True)
        settings_thread.daemon = True  # Dies when program ends.
        settings_thread.start()

    def add_settings_icon(self):
        self.settings_button.pack(side=ctk.RIGHT, anchor=ctk.SE, padx=10, pady=10)

    def clear_screen(self):
        for child in self.winfo_children():
            child.destroy()
        self.add_settings_icon()

    def load_join_window(self):
        join = JoinWindow(parent=self)
        join.pack(padx=20, pady=20)


class JoinWindow(ctk.CTkFrame):
    """
      └── Join
          ├── Settings
          ├── Load an existing network
          └── Bootstrap into a new network
    """
    def __init__(self, parent: MainGUI, fg_color="transparent", **kwargs):
        ctk.CTkFrame.__init__(self, parent, **kwargs)

        self.configure(fg_color=fg_color)

        join_title = ctk.CTkLabel(master=self, text="Join a Network", font=("Aharoni", 20, "bold"))
        join_title.pack(padx=50, pady=20)

        self.load_button = ctk.CTkButton(master=self, text="Load an existing network",
                                         command=self.handle_load_click)
        # self.load_button.grid(row=3, column=1, padx=5, pady=10)
        self.load_button.pack(padx=50, pady=10)

        self.bootstrap_button = ctk.CTkButton(master=self, text="Join a new network",
                                              command=self.handle_bootstrap_click)
        # self.bootstrap_button.grid(row=4, column=1, padx=5, pady=10)
        self.bootstrap_button.pack(padx=50, pady=10)

    def handle_load_click(self):
        # TODO: Create
        pass

    def handle_bootstrap_click(self):
        # TODO: Create
        pass


class BootstrapFrame(ctk.CTkFrame):
    def __init__(self, root, **kwargs):
        
        ctk.CTkFrame.__init__(self, root, **kwargs)
        
        ip_text = ctk.CTkLabel(master=self, text="IP Address: ")
        ip_text.grid(row=0, column=0, padx=5, pady=10, sticky="nsew")
        
        self.IP_entry = ctk.CTkEntry(master=self, width=150)
        self.IP_entry.grid(row=0, column=1, padx=5, pady=10, sticky="ew")

        port_text = ctk.CTkLabel(master=self, text="Port: ")
        port_text.grid(row=1, column=0, padx=5, pady=10, sticky="nsew")

        self.port_entry = ctk.CTkEntry(master=self, width=150)
        self.port_entry.grid(row=1, column=1, padx=5, pady=10, sticky="ew")

        self.connect_button = ctk.CTkButton(master=self, text="Connect",
                                            command=self.bootstrap)
        self.connect_button.grid(row=2, column=1, padx=5, pady=10)

    def bootstrap(self):
        """Attempts to bootstrap Kademlia connection from a given IP and port number"""
        print("[GUI] Connecting to bootstrap server...")


if __name__ == "__main__":
    # app = GUI()
    # app.load_bootstrap_window()
    # app.mainloop()

    app = MainGUI("light")
    app.mainloop()
    print("Done!")
