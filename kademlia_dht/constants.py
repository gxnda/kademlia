from dataclasses import dataclass


@dataclass
class Constants:
    DEBUG = False

    K = 20
    B = 5  # or 160 according to https://xlattice.sourceforge.net/components/protocol/kademlia/specs.html

    if DEBUG:
        A: int = 3
    else:
        A: int = 20

    REQUEST_TIMEOUT_SEC = 5  # 500ms
    ID_LENGTH_BYTES = 20
    ID_LENGTH_BITS = ID_LENGTH_BYTES * 8
    MAX_THREADS = 20
    RESPONSE_WAIT_TIME_MS = 10  # in ms
    BUCKET_REFRESH_INTERVAL_MS = 60 * 60 * 1000  # hourly in ms
    KEY_VALUE_REPUBLISH_INTERVAL_MS = 60 * 60 * 1000  # hourly in ms
    KEY_VALUE_EXPIRE_INTERVAL_MS = 60 * 60 * 1000  # hourly in ms
    ORIGINATOR_REPUBLISH_INTERVAL_MS = 24 * 60 * 60 * 1000  # every 24 hours in ms
    EXPIRATION_TIME_SEC = 24 * 60 * 60  # every 24 hours in seconds
    EVICTION_LIMIT = 5
    MAX_PORT_RETRIES = 100
    DHT_SERIALISED_SUFFIX = "dht"
    PICKLE_ENCODING = "latin1"
    PIECE_LENGTH= 262_144  # Standard piece length for BitTorrent pieces in bytes.

