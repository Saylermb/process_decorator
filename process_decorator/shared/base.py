import pickle
from multiprocessing import shared_memory


class SharedObjectBase:

    def __init__(self, size: int = 256, *args, **kwargs):
        self._share = shared_memory.SharedMemory(create=True,
                                                 size=size)
        self._args = args
        self._kwargs = kwargs
        self._clear_bytes = bytearray(b'\x00' * self._share.size)
        self._share.buf[:] = self._clear_bytes
        self._last_update_byte = 0
        self.__data = None
        self._type = None

    @property
    def data(self):
        if self.__data is None:
            if self._type is None:
                raise ValueError('dont found init type')
            self.__data = self._type(*self._args, **self._kwargs)
        self._refresh_from_buffer()
        return self.__data

    def _refresh_from_buffer(self):
        l_byte = self._share.buf[-1]
        if l_byte != self._last_update_byte:
            self.__data = pickle.loads(self._share.buf[:-1])
            self._last_update_byte = l_byte

    def commit(self):
        """
        update in shared buffer
        """
        clear = self._clear_bytes.copy()
        data = bytearray(pickle.dumps(self.data))
        clear[:len(data)] = data
        clear[-1] = self._last_update_byte + 1 if self._last_update_byte < 255 else 0
        self._share.buf[:] = clear

    def recreate(self):
        """
        clear and create new instance
        """
        self._refresh_from_buffer()
        self.__data = None
        self.commit()

    def __str__(self):
        return self.__data.__str__()

    def __repr__(self):
        return self.__data.__repr__()

    def __del__(self):
        self._share.close()
        self._share.unlink()

    def __getitem__(self, index):
        return self.data[index]
