
import os

DEFAULT_BLOCKSIZE = 16*1024

class ReverseFileIterator():
    def __init__(self, fd, blocksize=DEFAULT_BLOCKSIZE):
        self.fd = fd
        self.blocksize = blocksize
        self.fd.seek(0, os.SEEK_END)
        self.remaining_size = self.fd.tell()

    def __iter__(self):
        return self

    def __next__(self):
        blocksize = min(self.blocksize, self.remaining_size)
        if blocksize == 0:
            raise StopIteration()
        self.remaining_size -= blocksize
        self.fd.seek(self.remaining_size)
        return self.fd.read(blocksize)

    def __exit__(self):
        pass


class ReserveLineIterator():
    def __init__(self, fd, linsep, blocksize=DEFAULT_BLOCKSIZE):
        self.block_device = ReverseFileIterator(fd, blocksize)
        self.block_iterator = iter(self.block_device)
        self.buffer = linsep[:0]
        self.linsep = linsep

    def __iter__(self):
        return self

    def _read_more(self):
        self.buffer = next(self.block_iterator) + self.buffer
        if self.block_device.remaining_size == 0:
            self.buffer = self.linsep + self.buffer

    def __next__(self):
        try:
            last_newline = self.buffer.rindex(self.linsep, 0, -1)
        except ValueError:
            self._read_more()
            return self.__next__()  # probably the last line

        last_newline += 1
        line = self.buffer[last_newline:]
        self.buffer = self.buffer[:last_newline]

        if not line.strip():
            return self.__next__()  # probably the last line

        return line

    def __exit__(self):
        pass
