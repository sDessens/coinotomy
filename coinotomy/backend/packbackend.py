import os
import os.path
import struct

from coinotomy.utils.reservefileiterator import ReverseFileIterator

class PackStorageBackend(object):
    """
    Store trades in packed format. 16 bytes per row.

    Timestamps are stored as doubles (μs precision until year 3459),
    prices and volumes are stored as floats (4-digit precision until 1678 dollars/yen/bitcoin)

    If you want to track individual satoshi's, you may prefer to use doubles everywhere.
    """

    def __init__(self, name):
        self.template = os.path.expandvars(name)
        assert self.template
        self.fd = open(self._get_filename(), 'ab', 16)

    def __del__(self):
        self.unload()

    def unload(self):
        if self.fd:
            self.fd.close()
        self.fd = None

    def append(self, timestamp, price, vol):
        row = self._format_row(timestamp, price, vol)
        self.fd.write(row)

    def flush(self):
        self.fd.flush()

    def _format_row(self, timestamp, price, vol):
        return struct.pack(b"<dff", timestamp, price, vol)

    def _unformat_row(self, row):
        return struct.unpack(b"<dff", row)

    def _get_filename(self):
        return self.template + '.pack'

    def lines(self):
        self.fd.flush()
        unformat = self._unformat_row

        class iterator:
            def __init__(self, filename):
                self.fd = open(filename, 'rb')

            def __iter__(self):
                return self

            def __exit__(self):
                self.fd.close()

            def __next__(self):
                line = self.fd.read(16)
                if not line:
                    raise StopIteration()

                return unformat(line)

        return iterator(self._get_filename())

    def rlines(self):
        self.fd.flush()
        unformat = self._unformat_row

        class iterator:
            def __init__(self, filename):
                self.fd = open(filename, 'rb')
                self.reverse_file_iterator = iter(ReverseFileIterator(self.fd, blocksize=16))

            def __iter__(self):
                return self

            def __exit__(self):
                self.fd.close()

            def __next__(self):
                return unformat(next(self.reverse_file_iterator))

        return iterator(self._get_filename())

