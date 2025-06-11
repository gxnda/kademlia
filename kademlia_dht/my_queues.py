from typing import Optional


class InfiniteLinearQueue:
    def __init__(self):
        """
        Makes a linear queue with no maximum size.

        You may ask, what's the point of this? isn't this just a normal list?
        Basically, it is; but you have a dequeue method that doesn't error
        when trying to pop an empty list.

        I don't think this is actually needed,
        I also added this to get marks in my A-Level coursework.
        But it works, so I'll keep it :)
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
