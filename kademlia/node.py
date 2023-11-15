

class ID:
    def __init__(self, value: int):
        """
        Kademlia node ID: This is an integer from 0 to 2^160 - 1

        Args:
            value: (int) ID denary value
        """
        
        two_160 = 1461501637330902918203684832716283019655932542976
        self.MAX_ID = two_160 # 2^160
        self.MIN_ID = 0
        if not (value < self.MAX_ID and value > self.MIN_ID):
            raise ValueError("ID out of range - must a positive integer less "\
            "than 2^160.")
        self.value = value

    def hex(self) -> str:
        return hex(self.value)

    def denary(self) -> int:
        return self.value
    
    def bin(self) -> str:
        return bin(self.value)
    
    def __str__(self) -> str:
        return str(self.value)


class Node:
    """Node for K-Bucket"""
    def __init__(self, id: ID, IP: str, port: int) -> None:
        self.ip = IP
        self.port = port
        self.id = id
    

    def distance(self, id: int):
        return self.id.value ^ id


if __name__ == "__main__":
    id = ID(234525)
    print(id.hex())
    print(id.denary())
    print(id.bin())
    print(type(id.bin()))
    print(id.__str__())

    try:
        id = ID(2**160)
        print(id.hex())
    except ValueError as e:
        print(e)

    try:
        id = ID(2**160 - 2)
        print(id.hex())
        print(id.bin())
    except ValueError as e:
        print(e)

    node_1 = Node(id=ID(5), port=1234, IP="127.0.0.1")
    node_2 = Node(id=ID(7), IP="192.168.1.1", port=2346)
    print(node_1.distance(node_2.id.denary()))

    node_1 = Node(id=ID(5), port=1234, IP="127.0.0.1")
    node_2 = Node(id=ID(8), IP="192.168.1.1", port=2346)
    print(node_1.distance(node_2.id.denary()))

    node_1 = Node(id=ID(50), port=1234, IP="127.0.0.1")
    node_2 = Node(id=ID(700), IP="192.168.1.1", port=2346)
    print(node_1.distance(node_2.id.denary()))
    
    