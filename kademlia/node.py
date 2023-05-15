from time import time

class Node:
    """Node for K-Bucket"""
    def __init__(self, id: int, IP: str, port: int) -> None:
        self.ip = IP
        self.port = port
        self.id = int(id.hex(), 16)
    

    def distance(self, node):
        return self.id ^ node.id