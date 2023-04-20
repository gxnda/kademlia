class KademliaBucket:

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
        popped = self.bucket[0]
        self.bucket.pop(0)
        return popped
