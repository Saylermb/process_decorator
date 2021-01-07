from .base import SharedObjectBase


class Dict(SharedObjectBase):

    def __init__(self, size: int = 256, *args, **kwargs):
        super().__init__(size, *args, **kwargs)
        self._type = dict

    def get(self, id_):
        return self.data.get(id_)

    def set(self, id_, value):
        if value != self.data.get(id_):
            self.data[id_] = value
            self.commit()

    def __str__(self):
        return str(self.data)
