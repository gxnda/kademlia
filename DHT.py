
class KademliaBucket(object):
    """
    Based from Kademlia's "K-Buckets", which is essentially a queue with a maximum size k.
    if the bucket is not full, it will add any valid triples
    """
    def __init__(self, k_size):
        self.k_size = k_size
        self.bucket = []

    def add(self, IP, UDPport, NodeID):
        """Adds a list of IP, port, and nodeID to bucket"""
        if len(self.bucket) < self.k_size:
            self.bucket.append([IP, UDPport, NodeID])
        else:
            raise Exception("Bucket full.")

    def pop(self):
        """pops (removes) the top item of the bucket and returns it."""
        if len(self.bucket) == 0:
            raise IndexError("Bucket is empty, cannot pop.")
        popped = self.bucket[0]
        self.bucket.pop(0)
        return popped
