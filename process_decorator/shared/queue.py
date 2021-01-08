from collections import deque

from .base import SharedObjectBase


class Queue(SharedObjectBase):

    def __init__(self, size: int = 256, *args, **kwargs):
        super().__init__(size, *args, **kwargs)
        self._type = deque

    def pop(self):
        i = self.data.pop()
        self.commit()
        return i

    def popleft(self,):
        i = self.data.popleft()
        self.commit()
        return i

    def append(self, item):
        self.data.append(item)
        self.commit()

    def appendleft(self, item):
        self.data.appendleft(item)
        self.commit()
