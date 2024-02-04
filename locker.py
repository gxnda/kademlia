from threading import Lock


class WithLock:
    """
    Lock object that can be used in "with" statements.
    Example usage:
        lock = threading.Lock()
        with WithLock(lock):
            do_stuff()
        do_more_stuff()
    Based from the following code:
    https://www.bogotobogo.com/python/Multithread/python_multithreading_Synchronization_Lock_Objects_Acquire_Release.php
    https://www.geeksforgeeks.org/with-statement-in-python/
    """

    def __init__(self, lock: Lock) -> None:
        """
        Creates lock object to be used in __enter__ and __exit__.
        """
        self.lock = lock

    def __enter__(self) -> None:
        """
        Change the state to locked and returns immediately.
        """
        self.lock.acquire()

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        """
        Changes the state to unlocked; this is called from another thread.
        """
        self.lock.release()
