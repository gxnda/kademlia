import kademlia_protocol

class Bucket(object):
    """
    Based from Kademlia's "K-Buckets", which is essentially a queue with a maximum size k. As it is a queue, it relies on a last in last out system to ensure that all nodes get an even amount of traffic. 

    'If the node is not already in the appropriate k-bucket and the bucket has fewer than k entries, then the recipient just inserts the new sender at the tail of the list. If the appropriate k-bucket is full, then the recipient pings the k-bucket's least-recently seen node. If it fails to respond, it gets evicted from the k-bucket and the new sender is inserted at the tail.'

    'When a node recieves any message (request or reply) from another node, it updates the appropriate k-bucket for the senders Node ID.'
    """

    def __init__(self, k_size):
        if k_size < 0:
            raise ValueError("You may not create a k-bucket with a size limit less than 0.")
        self.k_size = k_size
        self.bucket = []

    def add(self, IP, port, NodeID) -> None:
        """Adds a list of IP, port, and nodeID to bucket"""
        if len(self.bucket) <= self.k_size:
            self.bucket.append([IP, port, NodeID])
        else:
            last_IP, last_port, _ = self.bucket[-1]
            is_available = kademlia_protocol.ping(last_IP, last_port)
            if not is_available:
                self.bucket[-1] = [IP, port, NodeID]

    def pop(self) -> list:
        """pops (removes) the top item of the bucket and returns it."""
        if len(self.bucket) == 0:
            raise IndexError("Bucket is empty, cannot pop.")
        popped = self.bucket[0]
        self.bucket.pop(0)
        return popped


def node_look_up():
    pass