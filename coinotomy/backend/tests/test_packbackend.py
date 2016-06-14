import os.path
import struct
import unittest

from coinotomy.backend.packbackend import PackStorageBackend
from coinotomy.backend.tests.test_backend_common import CommonBackend


def trunc(val):
    return struct.unpack(b"f", struct.pack(b'f', val))[0]


class TestPackBackend(unittest.TestCase, CommonBackend):

    FILENAME = "test.tmp"

    def _create(self):
        return PackStorageBackend(self.FILENAME)

    def setUp(self):
        self.maxDiff = None
        self.backend = self._create()

    def tearDown(self):
        self.backend.unload()
        os.unlink(self.backend._get_filename())

