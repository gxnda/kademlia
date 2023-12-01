from subprocess import check_call


try:
    import customtkinter as ctk
except ImportError:
    check_call(["pip", "install", "customtkinter"])
    import customtkinter as ctk

"""
├── User Interface
│   ├── Main Menu
│   ├── Download Menu
│   ├── Upload Menu
│   ├── Search Menu
│   └── Shared Files Menu

open UserInterface(), then check if the user is in a network already or not
- the k-buckets are stored in a JSON file, which is used to check this, 
and if the user is not in a network, the user is prompted to join a network.
"""


class GUI(ctk.CTk):
    def __init__(self):
        super().__init__()
        
        self.geometry("600x500")
        self.title("Kademlia P2P System")
        # self.set_appearance_mode("dark")

    def clear_screen(self):
        for widget in self.winfo_children():
            widget.destroy()

    def load_bootstrap_window(self):
        bootstrap = BootstrapFrame(self)
        bootstrap.pack(padx=20, pady=20)
        


class BootstrapFrame(ctk.CTkFrame):
    def __init__(self, root, **kwargs):
        
        super().__init__(root, **kwargs)
        
        IP_text = ctk.CTkLabel(master=self, text="IP Address: ")
        IP_text.grid(row=0, column=0, padx=5, pady=10, sticky="nsew")
        
        self.IP_entry = ctk.CTkEntry(master=self, width=150)
        self.IP_entry.grid(row=0, column=1, padx=5,pady=10, sticky="ew")


        port_text = ctk.CTkLabel(master=self, text="Port (optional): ")
        port_text.grid(row=1, column=0, padx=5, pady=10, sticky="nsew")

        self.port_entry = ctk.CTkEntry(master=self, width=150)
        self.port_entry.grid(row=1, column=1, padx=5, pady=10, sticky="ew")

        self.connect_button = ctk.CTkButton(master=self, text="Connect", command=self._connect)
        self.connect_button.grid(row=2, column=1, padx=5,pady=10)
    

    def _connect(self):
        """Attempts to bootstrap Kademlia connection from a given IP and port number"""
        print("Connecting to bootstrap server...")





if __name__ == "__main__":
    app = GUI()
    app.load_bootstrap_window()
    app.mainloop()