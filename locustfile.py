import asyncio
import multiprocessing
import os
import random
import time
import threading
from multiprocessing import Process
from tempfile import NamedTemporaryFile
from threading import Lock
from typing import Optional

from locust import HttpUser, task, between, events, tag

from kademlia.constants import Constants
from kademlia.contact import Contact
from kademlia.dht import DHT
from kademlia.id import ID
from kademlia.networking import AsyncServer
from kademlia.protocols import TCPSubnetProtocol
from kademlia.routers import Router, ParallelRouter
from kademlia.storage import VirtualStorage, BinaryFileStorage
from ui_helpers import create_logger

logger = create_logger(verbose=False)
valid_manifest_ids = []
local_ip = "127.0.0.1"
port = 7125

installed_file_paths = []
kp_process: Optional[Process] = None
server_started = multiprocessing.Value('b', False)
kp_server = AsyncServer(local_ip, port)


@events.quitting.add_listener
def cleanup_environment(environment):
    logger.info("Cleaning up servers...")
    if kp_process and kp_process.is_alive():
        kp_process.terminate()
        kp_process.join()
    logger.info("Known peer server terminated")
    for user in environment.runner.user_classes:
        if hasattr(user, 'dht'):
            user.dht.shutdown()


@events.init_command_line_parser.add_listener
def add_arguments(parser):
    parser.add_argument("--dht-stats", action="store_true", help="Enable DHT-specific statistics")
    parser.add_argument("--node-count", type=int, default=10, help="Number of DHT nodes to simulate")
    parser.add_argument("--file-size", type=int, default=1024, help="Size of test files in KB")
    parser.add_argument("--concurrent-ops", type=int, default=5, help="Number of concurrent operations per user")


@events.init.add_listener
def on_locust_init(environment, **kwargs):
    if environment.parsed_options:
        if environment.parsed_options.dht_stats:
            print("DHT statistics collection enabled")
        print(f"Simulating {environment.parsed_options.node_count} DHT nodes")
        print(f"Test file size: {environment.parsed_options.file_size} KB")
        print(f"Concurrent operations: {environment.parsed_options.concurrent_ops}")

# Custom stats collection
dht_stats = {}

def track_dht_request(name, start_time, response_length=0, exception=None):
    response_time = int((time.perf_counter() - start_time) * 1000)

    # Update stats dictionary
    if name not in dht_stats:
        dht_stats[name] = {"count": 0, "success": 0, "total_time": 0}

    dht_stats[name]["count"] += 1
    if not exception:
        dht_stats[name]["success"] += 1
    dht_stats[name]["total_time"] += response_time

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

def get_originator_storage_dir(id: ID | int):
    if isinstance(id, ID):
        id = id.value
    return f"files/{id}/originator"

def get_republish_storage_dir(id: ID | int):
    if isinstance(id, ID):
        id = id.value
    return f"files/{id}/republish"


def run_known_peer_server():
    """Function to run in a separate process for the known peer server"""
    try:
        # Initialize known peer inside the process, might not be necessary,
        # but weird shit happens in locust, so it can hide here to stop any
        # monkey patching shenanigans (Whole thing hung when trying to start
        # server in a thread)
        known_peer = DHT(
            id=ID(0),
            protocol=TCPSubnetProtocol(local_ip, port, 0),
            originator_storage=BinaryFileStorage(get_originator_storage_dir(0)),
            republish_storage=BinaryFileStorage(get_republish_storage_dir(0)),
            cache_storage=VirtualStorage(),
            router=Router()
        )

        # Create and run server
        global kp_server
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        kp_server.loop = loop

        loop.run_until_complete(kp_server.register_protocol(0, known_peer.node))
        loop.run_until_complete(kp_server.start())
        logger.info(f"Known peer server started on {local_ip}:{port}")
        loop.run_forever()
    except Exception as e:
        logger.error(f"Known peer server failed: {str(e)}")

@events.init.add_listener
def start_async_server_in_multiprocess(environment, **kwargs):
    global server_started
    with server_started.get_lock():
        if server_started.value:
            return

        server_started.value = True
    # Start known peer in a separate process
    kp_process = multiprocessing.Process(target=run_known_peer_server)
    kp_process.start()
    time.sleep(3)  # Give server time to start
    logger.info("Known peer server started in separate process")

known_peer_contact = Contact(ID(0), TCPSubnetProtocol(local_ip, port, 0))

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

        # Use ParallelRouter for better concurrency handling
        router_class = ParallelRouter if self.user_id % 2 == 0 else Router

        # Initialize DHT node
        start_time = time.perf_counter()
        self.dht = DHT(
            id=ID.random_id(),
            protocol=TCPSubnetProtocol(local_ip, port, self.subnet),
            originator_storage=BinaryFileStorage(get_originator_storage_dir(
                self.user_id)),
            republish_storage=BinaryFileStorage(get_republish_storage_dir(
                self.user_id)),
            cache_storage=VirtualStorage(),
            router=router_class()
        )
        while not server_started.value:
            logger.warning("sleeping until server up")
            time.sleep(1)
        logger.info(f"{kp_server}")
        asyncio.run(kp_server.register_protocol(self.subnet, self.dht.node))

        self.dht.bootstrap(known_peer_contact)
        track_dht_request("bootstrap", start_time)

        logger.info(f"[Locust {self.user_id}] Node initialized with ID {self.dht.our_contact.id}")

        # Get file size from command line args or use default
        file_size = 1024  # Default 1KB
        if hasattr(self.environment, "parsed_options") and self.environment.parsed_options.file_size:
            file_size = self.environment.parsed_options.file_size * 1024  # Convert KB to bytes

        # Create test files
        for i in range(5):  # Create 5 sample files per user
            with NamedTemporaryFile(delete=False) as temp_file:
                temp_file.write(os.urandom(file_size))
                self.files_to_store.append(temp_file.name)

        # Store one file to initialize the network
        if self.files_to_store:
            with self.counter_lock:
                start_time = time.perf_counter()
                manifest_id = self.dht.store_file(self.files_to_store[0])
                logger.info(f"[Locust] initially storing {manifest_id}")
                track_dht_request("initial_store", start_time, os.path.getsize(self.files_to_store[0]))


                valid_manifest_ids.append(manifest_id)

        # Create a large file for testing
        with NamedTemporaryFile(delete=False) as temp_file:
            temp_file.write(os.urandom(100 * Constants.PIECE_LENGTH))
            self.big_file = temp_file.name

        self.manifest_id = None
        logger.info(f"[Locust {self.user_id}] Started with {len(self.dht.node.bucket_list.contacts())} contacts")

        # # TODO: Remove (debugging)
        # logger.info(f"[Locust {self.user_id}] Sending ping to known peer")
        # error = known_peer.our_contact.protocol.ping(self.dht.our_contact)
        # logger.info(f"[Locust {self.user_id}] Ping result: {error.__dict__}")

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
    @tag('file_operations', 'store')
    def store_small_file(self):
        """Store a small file in the DHT"""
        start_time = time.perf_counter()
        try:
            logger.info(f"[Locust {self.user_id}] Storing small file")
            random_small_file = random.choice(self.files_to_store)
            manifest_id = self.dht.store_file(random_small_file)
            file_size = os.path.getsize(random_small_file)
            track_dht_request("store_small", start_time, file_size)
            logger.info(f"[Locust {self.user_id}] Stored small file with manifest ID {manifest_id}")
            with KademliaUser.counter_lock:
                valid_manifest_ids.append(manifest_id)
            return manifest_id
        except Exception as e:
            logger.error(f"[Locust {self.user_id}] Error storing small file: {str(e)}")
            track_dht_request("store_small", start_time, exception=e)
            raise

    @task
    @tag('file_operations', 'store', 'large_files')
    def store_big_file(self):
        """Store a large file in the DHT"""
        start_time = time.perf_counter()
        try:
            logger.info(f"[Locust {self.user_id}] Storing large file")
            manifest_id = self.dht.store_file(self.big_file)
            file_size = os.path.getsize(self.big_file)
            track_dht_request("store_large", start_time, file_size)
            logger.info(f"[Locust {self.user_id}] Stored large file with manifest ID {manifest_id}")
            with KademliaUser.counter_lock:
                valid_manifest_ids.append(manifest_id)
            return manifest_id
        except Exception as e:
            logger.error(f"[Locust {self.user_id}] Error storing large file: {str(e)}")
            track_dht_request("store_large", start_time, exception=e)
            raise

    @task(3)
    @tag('file_operations', 'retrieve')
    def retrieve_file(self):
        """Retrieve a file from the DHT"""
        start_time = time.perf_counter()
        try:
            if not valid_manifest_ids:
                logger.warning(f"[Locust {self.user_id}] No valid manifest IDs to retrieve")
                return None

            manifest_id = random.choice(valid_manifest_ids)
            logger.info(f"[Locust {self.user_id}] Retrieving file with manifest ID {manifest_id}")
            file_path = self.dht.download_file(manifest_id)
            file_size = os.path.getsize(file_path) if file_path else 0
            track_dht_request("retrieve", start_time, file_size)
            logger.info(f"[Locust {self.user_id}] Retrieved file to {file_path}")
            return file_path
        except Exception as e:
            logger.error(f"[Locust {self.user_id}] Error retrieving file: {str(e)}")
            track_dht_request("retrieve", start_time, exception=e)
            raise

    @task
    @tag('network', 'lookup')
    def node_lookup(self):
        """Perform a node lookup operation"""
        start_time = time.perf_counter()
        try:
            random_id = ID.random_id()
            logger.info(f"[Locust {self.user_id}] Looking up nodes close to {random_id}")
            close_nodes = self.dht._router.lookup(random_id,
                                                  self.dht._router.rpc_find_value)
            track_dht_request("node_lookup", start_time, len(close_nodes))
            logger.info(f"[Locust {self.user_id}] Found {len(close_nodes)} nodes close to {random_id}")
            return close_nodes
        except Exception as e:
            logger.error(f"[Locust {self.user_id}] Error during node lookup: {str(e)}")
            track_dht_request("node_lookup", start_time, exception=e)
            raise

    @task
    @tag('network', 'ping')
    def ping_random_node(self):
        """Ping a random node in the network"""
        start_time = time.perf_counter()
        try:
            contacts = self.dht.node.bucket_list.contacts()
            if not contacts:
                logger.warning(f"[Locust {self.user_id}] No contacts to ping")
                return

            contact: Contact = random.choice(contacts)
            logger.info(f"[Locust {self.user_id}] Pinging node {contact.id}")
            result = contact.protocol.ping(self.dht.our_contact)
            track_dht_request("ping", start_time)
            logger.info(f"[Locust {self.user_id}] Ping result: "
                        f"{'Success' if result.no_error() else 'Failed'}")
            return result
        except Exception as e:
            logger.error(f"[Locust {self.user_id}] Error pinging node: {str(e)}")
            track_dht_request("ping", start_time, exception=e)
            raise

    # @task
    @tag('concurrent', 'stress')
    def concurrent_operations(self):
        """Perform multiple operations concurrently"""
        if not hasattr(self.environment, "parsed_options") or not self.environment.parsed_options.concurrent_ops:
            concurrent_ops = 1  # Default
        else:
            concurrent_ops = self.environment.parsed_options.concurrent_ops

        logger.info(f"[Locust {self.user_id}] Starting {concurrent_ops} concurrent operations")
        start_time = time.perf_counter()

        operations = []
        threads = []

        # Define operations to run concurrently
        for _ in range(concurrent_ops):
            op = random.choice(['store', 'retrieve', 'lookup', 'ping'])
            operations.append(op)

            if op == 'store':
                t = threading.Thread(target=self.store_small_file)
            elif op == 'retrieve' and valid_manifest_ids:
                t = threading.Thread(target=self.retrieve_file)
            elif op == 'lookup':
                t = threading.Thread(target=self.node_lookup)
            else:  # ping
                t = threading.Thread(target=self.ping_random_node)

            threads.append(t)
            t.start()

        # Wait for all operations to complete
        for t in threads:
            t.join()

        total_time = time.perf_counter() - start_time
        track_dht_request("concurrent_ops", start_time, concurrent_ops)
        logger.info(f"[Locust {self.user_id}] Completed {concurrent_ops} concurrent operations in {total_time:.2f}s")
