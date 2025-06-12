import logging
import os
import random
from logging import Logger
from tempfile import NamedTemporaryFile
from threading import Lock

from locust import HttpUser, task, between

from kademlia_dht.constants import Constants
from kademlia_dht.dht import DHT
from kademlia_dht.id import ID
from kademlia_dht.networking import TCPSubnetServer
from kademlia_dht.protocols import TCPSubnetProtocol
from kademlia_dht.routers import Router
from kademlia_dht.storage import VirtualStorage, SecondaryJSONStorage

logger = logging.getLogger("__main__")
valid_manifest_ids = []
local_ip = "127.0.0.1"
port = 7125

installed_file_paths = []


known_peer = DHT(
    id=ID(0),
    protocol=TCPSubnetProtocol(local_ip, port, 0),
    originator_storage=SecondaryJSONStorage(
        f"files/{0}/originator_storage.json"),
    republish_storage=SecondaryJSONStorage(
        f"files/{0}/republish_storage.json"),
    cache_storage=VirtualStorage(),
    router=Router()
)
kp_server = TCPSubnetServer(
    (local_ip, port)
)
kp_server.register_protocol(0, known_peer.node)
kp_server.thread_start()

class KademliaUser(HttpUser):
    wait_time = between(1, 5)
    host = "http://localhost"
    counter = 1
    counter_lock = Lock()

    def on_start(self):
        with KademliaUser.counter_lock:
            self.user_id = KademliaUser.counter
            KademliaUser.counter += 1

        self.subnet = self.user_id
        self.dht = DHT(
            id=ID.random_id(),
            protocol=TCPSubnetProtocol(local_ip, port, self.subnet),
            originator_storage=SecondaryJSONStorage(
                f"files/{self.user_id}/originator_storage.json"),
            republish_storage=SecondaryJSONStorage(
                f"files/{self.user_id}/republish_storage.json"),
            cache_storage=VirtualStorage(),
            router=Router()
        )
        print("Setting up locust with ID", self.dht.our_contact.id)
        kp_server.register_protocol(self.subnet, self.dht.node)
        print("Bootstrapping from", known_peer.our_contact.id)
        self.dht.bootstrap(known_peer.our_contact)

        self.files_to_store = []
        for _ in range(5):  # Create 5 sample files per user
            with NamedTemporaryFile(delete=False) as temp_file:
                temp_file.write(os.urandom(1024))  # Write 1KB of random data
                self.files_to_store.append(temp_file.name)

        if self.files_to_store:
            with self.counter_lock:
                manifest_id = self.dht.store_file(self.files_to_store[0])
                valid_manifest_ids.append(manifest_id)

        with NamedTemporaryFile(delete=False) as temp_file:
            temp_file.write(os.urandom(100 * Constants.PIECE_LENGTH))
            self.big_file = temp_file.name

        self.manifest_id = None

    def on_stop(self):
        # Clean up temporary files
        try:
            os.unlink(self.big_file)
        except:
            pass

        for file_path in self.files_to_store:
            try:
                os.unlink(file_path)
            except:
                pass

    @task
    def store_small_file(self):
        # Implement file storage operation
        random_small_file = random.choice(self.files_to_store)
        self.dht.store_file(random_small_file)

    def store_big_file(self):
        # Implement file storage operation
        self.dht.store_file(self.big_file)

    @task(3)  # 3x more frequent than store
    def retrieve_real_file(self):
        logger.info("[Locust] Retrieving real file")
        # Implement file retrieval
        manifest_id = random.choice(valid_manifest_ids)
        installed_file_paths.append(self.dht.download_file(manifest_id))

    # @task
    def retrieve_fake_file(self):
        logger.info("[Locust] Retrieving fake file")
        # Implement file retrieval
        try:
            installed_file_paths.append(self.dht.download_file(ID.random_id()))
        except:
            pass