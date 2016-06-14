import os
import os.path

from coinotomy.utils.reservefileiterator import ReserveLineIterator

READ_SIZE = 16*1024  # 16K

class CsvStorageBackend(object):
    def __init__(self, name):
        self.template = os.path.expandvars(name)
        assert self.template
        self.fd = open(self._get_filename(), 'ab', 1)  # line buffered

    def __del__(self):
        self.unload()

    @staticmethod
    def extension():
        return ".csv"

    def unload(self):
        if self.fd:
            self.fd.close()
        self.fd = None

    def append(self, timestamp, price, vol):
        row = self._format_row(timestamp, price, vol) + b'\n'
        self.fd.write(row)

    def flush(self):
        self.fd.flush()

    def _format_row(self, timestamp, price, vol):
        return bytes("{},{},{}".format(
            self._format(timestamp, 4),
            self._format(price, 8),
            self._format(vol, 8)), 'ascii')

    def _unformat_row(self, row):

        try:
            ts, p, v = row.strip().split(b',')
        except:
            print(row)
            print(self.template)
            raise
        return float(ts), float(p), float(v)

    def _format(self, f, ndigits):
        str = "%%.%sf" % ndigits % f

        while -1 != str.find('.') and str[-1] in "0.":
            str = str[:-1]

        return str

    def _get_filename(self):
        return self.template + self.extension()

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
                line = self.fd.readline()
                if not line:
                    raise StopIteration()

                if not line.strip():
                    return self.__next__()  # probably the last line

                return unformat(line)

        return iterator(self._get_filename())

    def rlines(self):
        self.fd.flush()
        unformat = self._unformat_row

        class iterator:
            def __init__(self, filename):
                self.fd = open(filename, 'rb')
                self.reverse_line_iterator = ReserveLineIterator(self.fd, b'\n')
                self.iterator = iter(self.reverse_line_iterator)

            def __iter__(self):
                return self

            def __exit__(self):
                self.fd.close()

            def __next__(self):
                return unformat(next(self.iterator))

        return iterator(self._get_filename())

