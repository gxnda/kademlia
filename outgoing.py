import socket


def send_data(IP: str, port: int, data: str) -> None:
    with socket.create_connection(address=(IP, port)) as sock:
        sock.send(data.encode())


def ping(IP, port) -> bool:
    """Returns if a connection can be made."""

    try:
        port = int(port)
    except:
        raise ValueError("Port must be numeric.")
    
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock: #should automatically close once done
        try:
            sock.connect((IP, port))
            return True
        except socket.error:
            return False


def instruct_to_store(IP, port, data: str):
    to_send = "<STORE>" + data
    send_data(IP, port, to_send)


def find_node():
    """
    find_node takes a node ID as an argument, the recipient returns the k node triples it knows about closest to the target ID.
    """
    pass


def find_value():
    """
    Does the exact same thing as find_node, unless the recipient has recieved a STORE command for the key, where it returns just one value.
    """
    pass
    
