class RamdiskStorageBackend(object):
    def __init__(self):
        self.arr = []

    def unload(self):
        pass

    def append(self, timestamp, price, vol):
        self.arr.append((timestamp, price, vol))

    def flush(self):
        pass

    def lines(self):
        for line in self.arr:
            yield line

    def rlines(self):
        for line in self.arr[::-1]:
            yield line