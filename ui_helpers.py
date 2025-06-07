import argparse
import json
import logging
import os
import pickle
from threading import Thread
from os.path import exists

from requests import get
from sys import stdout

from kademlia_dht import helpers
from kademlia_dht.helpers import get_sha1_hash, get_manifest_hash
from kademlia_dht.node import Node
from kademlia_dht.protocols import TCPProtocol
from kademlia_dht.contact import Contact
from kademlia_dht.routers import ParallelRouter
from kademlia_dht.storage import SecondaryJSONStorage, VirtualStorage
from kademlia_dht.networking import TCPServer
from kademlia_dht.dht import DHT
from kademlia_dht.constants import Constants
from kademlia_dht.errors import IDMismatchError
from kademlia_dht.id import ID


def handle_terminal() -> tuple[bool, int, bool]:
    parser = argparse.ArgumentParser()
    parser.add_argument("--use_global_ip", action="store_true",
                        help="If the clients global IP should be used by the P2P network.")
    parser.add_argument("--port", type=int, required=False, default=7124)
    parser.add_argument("--verbose", action="store_true", required=False, default=False,
                        help="If logs should be verbose.")
    parser.add_argument("-v", action="store_true", required=False, default=False,
                        help="If logs should be verbose.")

    args = parser.parse_args()

    USE_GLOBAL_IP: bool = args.use_global_ip
    PORT: int = args.port
    VERBOSE: bool = args.v or args.verbose
    return USE_GLOBAL_IP, PORT, VERBOSE


def create_logger(verbose: bool) -> logging.Logger:
    logger = logging.getLogger(__name__)
    handler = logging.StreamHandler(stdout)

    # clear the log file
    with open("kademlia.log", "w"):
        pass

    if verbose:
        logging.basicConfig(filename="kademlia.log", level=logging.DEBUG,
                            format="%(asctime)s [%(levelname)s] %(message)s")
        handler.setLevel(logging.DEBUG)
    else:
        logging.basicConfig(filename="kademlia.log", level=logging.INFO,
                            format="%(asctime)s [%(levelname)s] %(message)s")
        handler.setLevel(logging.INFO)

    formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s', datefmt="%H:%M:%S")
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    return logger


def initialise_kademlia(USE_GLOBAL_IP, PORT, logger=None) -> tuple[DHT, TCPServer, Thread]:
    if logger:
        logger.info("Initialising Kademlia.")

    our_id = ID.random_id()
    if USE_GLOBAL_IP:  # Port forwarding is required.
        our_ip = get('https://api.ipify.org').content.decode('utf8')
    else:
        our_ip = "127.0.0.1"
    if logger:
        logger.info(f"Our hostname is {our_ip}.")

    if PORT:
        valid_port = PORT
    else:
        valid_port = helpers.get_valid_port()

    if logger:
        logger.info(f"Port free at {valid_port}, creating our node here.")

    protocol = TCPProtocol(
        url=our_ip, port=valid_port
    )

    our_node = Node(
        contact=Contact(
            id=our_id,
            protocol=protocol
        ),
        storage=SecondaryJSONStorage(f"{our_id.value}/node.json"),
        cache_storage=VirtualStorage()
    )

    # Make directory of our_id at current working directory.
    create_dir_at = os.path.join(os.getcwd(), str(our_id.value))
    logger.info(f"Making directory at {create_dir_at}")

    if not exists(create_dir_at):
        os.mkdir(create_dir_at)
    dht: DHT = DHT(
        id=our_id,
        protocol=protocol,
        originator_storage=SecondaryJSONStorage(f"{our_id.value}/originator_storage.json"),
        republish_storage=SecondaryJSONStorage(f"{our_id.value}/republish_storage.json"),
        cache_storage=VirtualStorage(),
        router=ParallelRouter(our_node)
    )

    server = TCPServer(our_node)
    server_thread = server.thread_start()

    return dht, server, server_thread
