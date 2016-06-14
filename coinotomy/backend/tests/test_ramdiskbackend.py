import os.path
import struct
import unittest

from coinotomy.backend.ramdiskbackend import RamdiskStorageBackend
from coinotomy.backend.tests.test_backend_common import CommonBackend


def trunc(val):
    return struct.unpack(b"f", struct.pack(b'f', val))[0]


class TestPackBackend(unittest.TestCase, CommonBackend):

    FILENAME = "test.tmp"

    def _create(self):
        return RamdiskStorageBackend()

    def setUp(self):
        self.maxDiff = None
        self.backend = self._create()

    def tearDown(self):
        self.backend.unload()

    def test_reopen(self):
        pass  # override. Doesn't need to work for RamdiskStorageBackend

