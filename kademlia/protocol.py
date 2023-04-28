import socket

def ping(IP, port) -> bool:
    """Returns if a connection can be made."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        try:
            sock.connect((IP, port))
            return True
        except socket.error:
            return False


def store():
    pass

def find_node():
    pass

def find_value():
    pass