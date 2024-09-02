from kademlia_dht.dictionaries import TorrentFile


class TorrentFileHandler:
    """
    A torrent file contains a list of files and integrity metadata about all the pieces,
    and optionally contains a large list of trackers.

    pieces — a hash list, i.e., a concatenation of each piece's SHA-1 hash. As SHA-1 returns a 160-bit hash,
    pieces will be a string whose length is a multiple of 20 bytes. If the torrent contains multiple files,
    the pieces are formed by concatenating the files in the order they appear in the files dictionary
    (i.e., all pieces in the torrent are the full piece length except for the last piece, which may be shorter).

    A torrent is uniquely identified by an infohash, a SHA-1 hash calculated over the contents of the info
    dictionary in bencode form. Changes to other portions of the torrent do not affect the hash.
    This hash is used to identify the torrent to other peers via DHT and to the tracker.
    It is also used in magnet links.
    """
    def __init__(self, torrent_file: TorrentFile):
