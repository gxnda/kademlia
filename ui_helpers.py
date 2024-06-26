import argparse
import logging
from sys import stdout


def handle_terminal() -> tuple[bool, int, bool]:
    parser = argparse.ArgumentParser()
    parser.add_argument("--use_global_ip", action="store_true",
                        help="If the clients global IP should be used by the P2P network.")
    parser.add_argument("--port", type=int, required=False, default=7124)
    parser.add_argument("-verbose", action="store_true", required=False, default=False,
                        help="If logs should be verbose.")
    parser.add_argument("--v", action="store_true", required=False, default=False,
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
