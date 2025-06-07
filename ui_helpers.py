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


def store_file(file_to_upload: str, dht: DHT) -> ID:
    filename = os.path.basename(file_to_upload)
    piece_size = Constants.PIECE_LENGTH  # in bytes
    encoded_filename = filename.encode(Constants.PICKLE_ENCODING)
    piece_dict: dict[int, bytes] = {get_sha1_hash(encoded_filename): encoded_filename}
    with open(file_to_upload, "rb") as f:
        file_read = False
        while not file_read:
            file_piece: bytes = f.read(piece_size)
            if file_piece:
                piece_key: int = get_sha1_hash(file_piece)
                piece_dict[piece_key] = file_piece
            else:
                file_read = True

    manifest_list: list[int] = list(piece_dict.keys())
    manifest_key: int = get_manifest_hash(manifest_list)

    # Store the manifest
    dht.store(ID(manifest_key), bytes(manifest_list).decode(
        Constants.PICKLE_ENCODING))

    # Store each of the pieces
    for piece_key in piece_dict:
        dht.store(ID(piece_key), piece_dict[piece_key].decode(Constants.PICKLE_ENCODING))


def download_file(manifest_id: ID, dht: DHT) -> str:
    """
    Downloads a given file from the given DHT. It does this by taking the
    manifest ID, which points to an encoded list of all piece's keys.

    It then uses this list to download each piece of the file.

    The manifest list is NOT sorted, so the key order given is the order of
    the file.
    """


    found, contacts, val = dht.find_value(key=manifest_id)
    # val will be a 'latin1' pickled dictionary {filename: str, file: bytes}
    if not found:
        raise IDMismatchError(str(manifest_id))
    else:
        manifest_list: list[int] = list(val.encode(Constants.PICKLE_ENCODING))
        filename_key: int = manifest_list.pop(0)
        found, contacts, filename = dht.find_value(ID(filename_key))
        if not found:
            raise IDMismatchError("Filename not found on network")

        install_path = os.path.join(os.getcwd(), filename)
        with open(install_path, "wb") as f:
            for piece_key in manifest_list:
                found, contacts, val = dht.find_value(key=ID(piece_key))
                if not found:
                    raise IDMismatchError(str(ID(piece_key)))

                byte_piece: bytes = val.encode(Constants.PICKLE_ENCODING)
                f.write(byte_piece)

        return str(install_path)


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
