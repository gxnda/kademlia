import logging
import os
import random
import time
from tempfile import NamedTemporaryFile
from threading import Lock

from locust import HttpUser, task, between, events

from kademlia_dht.constants import Constants
from kademlia_dht.dht import DHT
from kademlia_dht.id import ID
from kademlia_dht.networking import TCPSubnetServer
from kademlia_dht.protocols import TCPSubnetProtocol
from kademlia_dht.routers import Router
from kademlia_dht.storage import VirtualStorage, SecondaryJSONStorage

from locust.runners import STATE_STOPPING, STATE_STOPPED, STATE_CLEANUP

logger = logging.getLogger("__main__")
valid_manifest_ids = []
local_ip = "127.0.0.1"
port = 7125

installed_file_paths = []


@events.quitting.add_listener
def cleanup_environment(environment):
    logger.info("Cleaning up servers...")
    kp_server.shutdown()
    for user in environment.runner.user_classes:
        if hasattr(user, 'dht'):
            user.dht.shutdown()


@events.init_command_line_parser.add_listener
def add_arguments(parser):
    parser.add_argument("--dht-stats", action="store_true", help="Enable DHT-specific statistics")


@events.init.add_listener
def on_locust_init(environment, **kwargs):
    if environment.parsed_options and environment.parsed_options.dht_stats:
        print("DHT statistics collection enabled")

# Custom stats collection
dht_stats = {}

def track_dht_request(name, start_time, response_length=0, exception=None):
    response_time = int((time.perf_counter() - start_time) * 1000)

    if exception:
        events.request.fire(
            request_type="DHT",
            name=name,
            response_time=response_time,
            response_length=response_length,
            exception=exception,
        )
    else:
        events.request.fire(
            request_type="DHT",
            name=name,
            response_time=response_time,
            response_length=response_length,
        )

@events.quitting.add_listener
def print_dht_stats(environment, **kwargs):
    if environment.parsed_options and environment.parsed_options.dht_stats:
        print("\nDHT Detailed Statistics:")
        print("="*50)
        for name, stats in dht_stats.items():
            avg_time = stats["total_time"] / max(stats["count"], 1)
            success_rate = (stats["success"] / stats["count"]) * 100 if stats["count"] > 0 else 0
            print(f"{name}:")
            print(f"  Requests: {stats['count']}")
            print(f"  Success: {stats['success']} ({success_rate:.1f}%)")
            print(f"  Avg Time: {avg_time:.2f}ms")
            print("-"*50)


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

logger.info("[Known peer] started.")

class KademliaUser(HttpUser):
    wait_time = between(5, 10)
    host = "http://localhost"
    counter = 1
    counter_lock = Lock()

    def on_start(self):
        with KademliaUser.counter_lock:
            self.user_id = KademliaUser.counter
            KademliaUser.counter += 1

        self.files_to_store = []
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
        logger.info("Bootstrapping done!")

        logger.info(f"our buckets after bootstrapping:"
                    f" {self.dht.node.bucket_list}")

        logger.info(f"[Locust {self.user_id}] Started")

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
        start_time = time.perf_counter()
        try:
            logger.info(f"[Locust {self.user_id}] Storing small file")
            random_small_file = random.choice(self.files_to_store)
            manifest_id = self.dht.store_file(random_small_file)
            file_size = os.path.getsize(random_small_file)
            track_dht_request("store_small", start_time, file_size)
            return manifest_id
        except Exception as e:
            track_dht_request("store_small", start_time, exception=e)
            raise

    @task
    def store_big_file(self):
        start_time = time.perf_counter()
        try:
            logger.info(f"[Locust {self.user_id}] Storing small file")
            manifest_id = self.dht.store_file(self.big_file)
            file_size = os.path.getsize(self.big_file)
            track_dht_request("store_small", start_time, file_size)
            return manifest_id
        except Exception as e:
            track_dht_request("store_small", start_time, exception=e)
            raise

    @task(3)
    def retrieve_real_file(self):
        start_time = time.perf_counter()
        try:
            logger.info(f"[Locust {self.user_id}] Retrieving real file")
            manifest_id = random.choice(valid_manifest_ids)
            file_path = self.dht.download_file(manifest_id)
            file_size = os.path.getsize(file_path) if file_path else 0
            track_dht_request("retrieve", start_time, file_size)
            return file_path
        except Exception as e:
            track_dht_request("retrieve", start_time, exception=e)
            raise

    # @task
    # def retrieve_fake_file(self):
    #     logger.info("[Locust] Retrieving fake file")
    #     # Implement file retrieval
    #     try:
    #         installed_file_paths.append(self.dht.download_file(ID.random_id()))
    #     except:
    #         pass