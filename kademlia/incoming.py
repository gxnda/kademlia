import socket


class Server:

    def __init__(self, port=7125) -> None:
        addr = ("", port)
        self.server = socket.create_server(address=addr, family="AF_INET")
        self.listen_data = None

    def listen(self) -> bytes:
        """
        Designed for use in threaded systems, where the data can be fetched from self.listen_data.
        If not used in threading, it will also return this data.

        Due to how data is sent in outgoing.send_data, this can be decoded with .decode()
        """
        listen_data = self.server.recv(2048)
        self.listen_data = listen_data
        return listen_data
    
    def shut_down(self) -> None:
        self.server.close()
