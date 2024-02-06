from typing import Optional


class InfiniteLinearQueue:
    def __init__(self):
        """
        You may ask, what's the point of this? isn't this just a normal list?
        Basically, it is; but you have a dequeue method.
        """
        self.__items = []

    def is_empty(self):
        return len(self.__items) == 0

    def enqueue(self, item):
        self.__items.append(item)
        return True

    def dequeue(self) -> Optional[any]:
        if not self.is_empty():
            return self.__items.pop(0)
        else:
            return None


class LinearQueue(object):

    def __init__(self, qSize):  # Parameters go into __init__
        # __VarName makes the variable invisible to the user (protection modifier)
        self.__q = [None] * qSize
        self.__front = 0
        self.__rear = -1  # enQueue is never called for -1, as it increases by 1 before calling it
        self.__size = 0
        self.__max_size = qSize

    def enqueue(self, item):
        if self.is_full():
            return False
        else:
            self.__rear += 1
            self.__q[self.__rear] = item
            self.__size += 1
            return True

    def dequeue(self):
        if self.is_empty():
            return None
        else:
            self.__size -= 1
            item = self.__q[self.__front]
            self.__front += 1
            return item

    def is_full(self):
        return self.__rear >= self.__max_size - 1  # Returns boolean

    def is_empty(self):
        return self.__size == 0  # Returns boolean

    def __str__(self):
        output = ""
        for index in range(self.__front, self.__rear + 1):
            output += str(self.__q[index]) + " "
        return output[:-1]

