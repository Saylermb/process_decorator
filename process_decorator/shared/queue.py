from collections import deque

from .base import SharedObjectBase


class Queue(SharedObjectBase):

    def __init__(self, size: int = 256, *args, **kwargs):
        super().__init__(size, *args, **kwargs)
        self._type = deque
        _enable_data_attrs = ('count', 'maxlen', 'reverse')

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

    def __getattr__(self, item):
        if item in dir(self):
            return super().__getattr__(item)
        elif item in self._enable_data_attrs:
            return getattr(self.data, item)
        else:
            return super().__getattr__(item)
