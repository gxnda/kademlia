from typing import Optional


class InfiniteLinearQueue:
    def __init__(self):
        """
        Makes a linear queue with no maximum size.

        You may ask, what's the point of this? isn't this just a normal list?
        Basically, it is; but you have a dequeue method.
        """
        self.__items = []

    def is_empty(self):
        """
        Returns if the queue is empty or not.
        :return:
        """
        return len(self.__items) == 0

    def enqueue(self, item) -> None:
        """
        Adds an item to the queue.
        :param item:
        :return:
        """
        self.__items.append(item)

    def dequeue(self) -> Optional[any]:
        """
        Removes an item to the queue and returns it – returns None if it is not in the queue.
        :return:
        """
        if not self.is_empty():
            return self.__items.pop(0)
        else:
            return None


class LinearQueue(object):

    def __init__(self, queue_size: int):
        """
        Initialises the queue with a given maximum size.
        :param queue_size:
        """
        self.__queue = [None] * queue_size
        self.__front = 0
        self.__rear = -1  # enQueue is never called for -1, as it increases by 1 before calling it
        self.__size = 0
        self.__max_size = queue_size

    def enqueue(self, item):
        """
        Adds item to the tail of the queue. Returns True if success, False if failure.
        :param item:
        :return:
        """
        if self.is_full():
            return False
        else:
            self.__rear += 1
            self.__queue[self.__rear] = item
            self.__size += 1
            return True

    def dequeue(self):
        """
        Removes an item from the front of the list and returns it. Returns None if the list is empty.
        :return:
        """
        if self.is_empty():
            return None
        else:
            self.__size -= 1
            item = self.__queue[self.__front]
            self.__front += 1
            return item

    def is_full(self):
        """
        Returns if the list is full.
        :return:
        """
        return self.__rear >= self.__max_size - 1  # Returns boolean

    def is_empty(self):
        """
        Returns if the list is empty.
        :return:
        """
        return self.__size == 0  # Returns boolean

    def __str__(self):
        """
        Prints each element of the queue separated by whitespace.
        :return:
        """
        output = ""
        for index in range(self.__front, self.__rear + 1):
            output += str(self.__queue[index]) + " "
        return output[:-1]
