import logging

logger = logging.getLogger("__main__")


class KademliaError(Exception):
    pass


class DataDecodingError(KademliaError):
    pass


class TooManyContactsError(KademliaError):
    """Raised when a contact is added to a full k-bucket."""
    pass


class OutOfRangeError(KademliaError):
    """Raised when a contact is added to a k-bucket that is out of range."""
    pass


class OurNodeCannotBeAContactError(KademliaError):
    """Raised when a contact added has the same ID as the client."""


class AllKBucketsAreEmptyError(KademliaError):
    """Raised when no KBuckets can be iterated through."""


class SendingQueryToSelfError(KademliaError):
    """Raised when a Query (RPC Call) is sent to ourselves."""
    pass


class SenderIsSelfError(KademliaError):
    """Raised when trying to send certain RPC commands, if sender is us."""
    pass


class BucketDoesNotContainContactToEvictError(KademliaError):
    pass


class NoNonEmptyBucketsError(KademliaError):
    pass


class RPCError(KademliaError):
    """
    Possible errors for RPC commands.
    """

    def __init__(self,
                 error_message: str | None = None,
                 timeout_error: bool = False,
                 id_mismatch_error: bool = False,
                 peer_error: bool = False,
                 peer_error_message: str | None = None
                 ):
        """
        Initialises an RPCError method – having all these error types together allows checking for RPCErrors
        very easy, and still readable.
        :param error_message:
        :param timeout_error:
        :param id_mismatch_error:
        :param peer_error:
        :param peer_error_message:
        """
        super().__init__(error_message)
        self.protocol_error_message: str | None = error_message

        if error_message:
            self.protocol_error = True
        else:
            self.protocol_error = False

        self.timeout_error = timeout_error
        self.id_mismatch_error = id_mismatch_error
        self.peer_error = peer_error
        self.peer_error_message: str | None = peer_error_message

        if self.peer_error_message and not self.peer_error:
            raise ValueError("Parameter peer error message requires a peer error.")

    def has_error(self) -> bool:
        """
        Returns True if any type of error is true, else False.
        :return:
        """
        return self.timeout_error or \
            self.protocol_error or \
            self.id_mismatch_error or \
            self.peer_error

    def __str__(self):
        """
        Returns error message, or “No error” if there is none.
        :return:
        """
        if self.has_error():
            if self.protocol_error:
                return f"Protocol error: {self.protocol_error_message}"
            elif self.peer_error:
                return f"Peer error: {self.peer_error_message}"
            elif self.timeout_error:
                return "Timeout error."
            elif self.id_mismatch_error:
                return "ID mismatch error."
            else:
                return "Unknown error."
        else:
            return "No error."

    @classmethod
    def no_error(cls):
        return cls()


class ProtocolError(RPCError):
    pass


class PeerError(RPCError):
    pass


class TimeoutError(RPCError):
    pass


class IDMismatchError(RPCError):
    pass


class ValueCannotBeNoneError(KademliaError):
    """
    Raised when a value is None, when everything was meant to have gone OK.
    There is a risk of this being purposely triggered maliciously to shut down nodes on the network.
    I'm not sure what to do in that situation; this is an issue that should be discussed. TODO: Discuss.
    """


class UnknownRequestError(KademliaError):
    pass


class IncorrectProtocolError(KademliaError):
    pass
