

class DataDecodingError(Exception):
    pass

class TooManyContactsError(Exception):
    """Raised when a contact is added to a full k-bucket."""
    pass


class OutOfRangeError(Exception):
    """Raised when a contact is added to a k-bucket that is out of range."""
    pass


class OurNodeCannotBeAContactError(Exception):
    """Raised when a contact added has the same ID as the client."""


class AllKBucketsAreEmptyError(Exception):
    """Raised when no KBuckets can be iterated through."""


class SendingQueryToSelfError(Exception):
    """Raised when a Query (RPC Call) is sent to ourselves."""
    pass


class SenderIsSelfError(Exception):
    """Raised when trying to send certain RPC commands, if sender is us."""
    pass


class BucketDoesNotContainContactToEvictError(Exception):
    pass


class RPCError(Exception):
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
        print(self.timeout_error, self.protocol_error, self.id_mismatch_error, self.peer_error)
        return self.timeout_error or \
            self.protocol_error or \
            self.id_mismatch_error or \
            self.peer_error

    def __str__(self):
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

    @staticmethod
    def no_error():
        pass


class ValueCannotBeNoneError(Exception):
    """
    Raised when a value is None, when everything was meant to have gone OK.
    There is a risk of this being purposely triggered maliciously to shut down nodes on the network.
    I'm not sure what to do in that situation.
    TODO: Talk about this in the write-up.
    """


class UnknownRequestError(Exception):
    pass
