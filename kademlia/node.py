from time import time

class Node:
    """Node for K-Bucket"""
    def __init__(self, IP: str, port: int, id: str) -> None:
        self.ip = IP
        self.port = port
        self.id = id
    
    