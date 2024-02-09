import customtkinter as ctk
from PIL import Image


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


class GUI(ctk.CTk):
    def __init__(self):
        ctk.CTk.__init__(self)

        self.geometry("600x500")
        self.title("Kademlia")
        # self.set_appearance_mode("dark")
        dark_icon = Image.open(r"assets/settings_icon_light.png")
        light_icon = Image.open(r"assets/settings_icon_dark.png")
        settings_icon = ctk.CTkImage(light_image=light_icon, dark_image=dark_icon, size=(30, 30))
        self.settings_button = ctk.CTkButton(self, image=settings_icon, text="",
                                             bg_color="transparent", fg_color="transparent",
                                             width=28, command=self.open_settings)
        self.load_join_window()
        self.add_settings()

    def open_settings(self):
        # TODO: Create
        pass

    def add_settings(self):
        self.settings_button.pack(side=ctk.RIGHT, anchor=ctk.SE, padx=10, pady=10)

    def clear_screen(self):
        for child in self.winfo_children():
            child.destroy()
        self.add_settings()

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
    def __init__(self, parent: GUI, **kwargs):
        ctk.CTkFrame.__init__(self, parent, **kwargs)

        self.configure(fg_color="transparent")

        join_title = ctk.CTkLabel(master=self, text="Join a Network", font=("Aharoni", 20, "bold"))
        join_title.pack(padx=50, pady=20)

        self.settings_button = ctk.CTkButton(master=self, text="Settings", command=self.handle_settings_click)
        # self.settings_button.grid(row=2, column=1, padx=5, pady=10)
        self.settings_button.pack(padx=50, pady=10)

        self.load_button = ctk.CTkButton(master=self, text="Load an existing network", command=self.handle_load_click)
        # self.load_button.grid(row=3, column=1, padx=5, pady=10)
        self.load_button.pack(padx=50, pady=10)

        self.bootstrap_button = ctk.CTkButton(master=self, text="Join a new network", command=self.handle_bootstrap_click)
        # self.bootstrap_button.grid(row=4, column=1, padx=5, pady=10)
        self.bootstrap_button.pack(padx=50, pady=10)

    def handle_settings_click(self):
        # TODO: Create.
        pass

    def handle_load_click(self):
        # TODO: Create
        pass

    def handle_bootstrap_click(self):
        # TODO: Create
        pass


class BootstrapFrame(ctk.CTkFrame):
    def __init__(self, root, **kwargs):
        
        ctk.CTkFrame.__init__(self, root, **kwargs)
        
        IP_text = ctk.CTkLabel(master=self, text="IP Address: ")
        IP_text.grid(row=0, column=0, padx=5, pady=10, sticky="nsew")
        
        self.IP_entry = ctk.CTkEntry(master=self, width=150)
        self.IP_entry.grid(row=0, column=1, padx=5,pady=10, sticky="ew")

        port_text = ctk.CTkLabel(master=self, text="Port: ")
        port_text.grid(row=1, column=0, padx=5, pady=10, sticky="nsew")

        self.port_entry = ctk.CTkEntry(master=self, width=150)
        self.port_entry.grid(row=1, column=1, padx=5, pady=10, sticky="ew")

        self.connect_button = ctk.CTkButton(master=self, text="Connect", command=self.bootstrap)
        self.connect_button.grid(row=2, column=1, padx=5,pady=10)
    

    def bootstrap(self):
        """Attempts to bootstrap Kademlia connection from a given IP and port number"""
        print("[GUI] Connecting to bootstrap server...")


if __name__ == "__main__":
    # app = GUI()
    # app.load_bootstrap_window()
    # app.mainloop()

    app = GUI()
    app.mainloop()
