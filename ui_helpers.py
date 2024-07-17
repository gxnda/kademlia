import argparse
import json
import logging
import os
import pickle
from sys import stdout

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


def store_file(file_to_upload: str, dht) -> ID:
    filename = os.path.basename(file_to_upload)
    with open(file_to_upload, "rb") as f:
        file_contents: bytes = f.read()

    # val will be a 'latin1' pickled dictionary {filename: str, file: bytes}
    val: str = json.dumps({"filename": filename, "file": file_contents.decode(Constants.PICKLE_ENCODING)})
    del file_contents  # free up memory, file_contents could be pretty big.

    id_to_store_to = ID.random_id()
    dht.store(id_to_store_to, val)

    return id_to_store_to


def download_file(id_to_download: ID, dht) -> str:
    found, contacts, val = dht.find_value(key=id_to_download)
    # val will be a 'latin1' pickled dictionary {filename: str, file: bytes}
    if not found:
        raise IDMismatchError("File ID not found on the network.")
    else:
        # TODO: It might be a better idea to use JSON to send values.
        val_bytes: bytes = val.encode(Constants.PICKLE_ENCODING)  # TODO: Add option for changing this in settings.

        # "pickle.loads()" is very insecure and can lead to arbitrary code execution, the val received
        #   could be maliciously crafted to allow for malicious code execution because it compiles and creates
        #   a python object.
        file_dict: dict = pickle.loads(val_bytes)  # TODO: Make secure.
        if not isinstance(file_dict, dict):
            raise TypeError("The file downloaded is formatted incorrectly.")

        filename: str = file_dict["filename"]
        if not isinstance(filename, str):
            raise TypeError("The file downloaded is formatted incorrectly.")

        file_bytes: bytes = file_dict["file"]
        if not isinstance(file_bytes, bytes):
            raise TypeError("The file downloaded is formatted incorrectly.")

        del file_dict  # Free up memory.

        # get current working directory
        cwd = os.getcwd()  # TODO: Add option to change where it is installed to.

        install_path = os.path.join(cwd, filename)  # writes the file to the current working directory

        with open(install_path, "wb") as f:
            f.write(file_bytes)

        return str(install_path)


