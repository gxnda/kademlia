from locust import HttpUser, task, between
from kademlia_dht.dht import DHT
from kademlia_dht.id import ID
from kademlia_dht.contact import Contact
from kademlia_dht.protocols import VirtualProtocol
from kademlia_dht.routers import Router
from kademlia_dht.storage import VirtualStorage


class KademliaUser(HttpUser):
    wait_time = between(0.1, 0.5)
    host = "http://localhost"

    def on_start(self):
        self.dht = DHT(ID.random_id(), VirtualProtocol(), storage_factory=VirtualStorage, router=Router()) # Initialize node
        self.manifest_id = None

    @task
    def store_file(self):
        # Implement file storage operation
        self.manifest_id = self.dht.store_file("test_file.txt")

    @task(3)  # 3x more frequent than store
    def retrieve_file(self):
        # Implement file retrieval
        if self.manifest_id:
            self.dht.download_file(self.manifest_id)
