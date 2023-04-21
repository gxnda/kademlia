import socket

def send_data(IP: str, port: int, data: str) -> None:
    with socket.create_connection(address=(IP, port)) as connection:
        connection.send(data.encode())

def listen(mmmmm):
    mmmmm.recv(1024)
    