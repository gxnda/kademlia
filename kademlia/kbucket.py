import protocol
from node import Node


class Bucket(object):
    """
    Based from Kademlia's "K-Buckets", which is essentially a queue with a maximum size k. 
    As it is a queue, it relies on a last in last out system to ensure that all nodes get an 
    even amount of traffic. 

    'If the node is not already in the appropriate k-bucket and the bucket has fewer than k 
    entries, then the recipient just inserts the new sender at the tail of the list. 
    If the appropriate k-bucket is full, then the recipient pings the k-bucket's 
    least-recently seen node. If it fails to respond, it gets evicted from the k-bucket and 
    the new sender is inserted at the tail.'

    'When a node recieves any message (request or reply) from another node, it updates the 
    appropriate k-bucket for the senders Node ID.'
    """

    def __init__(self, k_size):
        if k_size < 0:
            raise ValueError("You may not create a k-bucket with a size limit less than 0.")
        self.k_size = k_size
        self.bucket = []

    def isFull(self) -> bool:
        return self.bucket >= self.k_size

    def isEmpty(self) -> bool:
        return len(self.bucket) == 0

    def add(self, node: Node) -> None:
        """Adds a list of IP, port, and nodeID to bucket"""
        if len(self.bucket) <= self.k_size:
            self.bucket.append(node)
        else:
            last_IP, last_port, _ = self.bucket[-1].ip, self.bucket[-1].port
            is_available = protocol.ping(last_IP, last_port)
            if not is_available:
                self.bucket[-1] = node

    def pop(self) -> dict:
        """pops (removes) the top item of the bucket and returns it."""
        if self.isEmpty():
            raise IndexError("Bucket is empty, cannot pop.")
        return self.bucket.pop(0)
    
    def move_front_to_tail(self):
        self.bucket.append(self.bucket[0])
        self.pop()
    
    def add_contact(self, node):
        """
        Adds contact to k-bucket, as according to this diagram:
        https://www.syncfusion.com/books/kademlia_protocol_succinctly/Images/the-add-contact-algorithm.png
        """
        #does the node exist?
        if node in self.bucket:
            index = self.bucket.index(node)
            #don't need to check if empty because something is in there
            removed_node = self.bucket.pop(index)
            self.bucket.append(removed_node)
            #done
            return
        
        if self.isFull():
            last_seen = self.bucket[0]  # last seen are first in queue (queuing for the longest)
            if protocol.ping(last_seen.ip, last_seen.port):
                self.move_front_to_tail()
                return
            else:
                pass
        else:
            pass
        
