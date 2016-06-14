import os.path
import unittest
from math import pi, e

from coinotomy.backend.csvbackend import CsvStorageBackend
from coinotomy.backend.tests.test_backend_common import CommonBackend

class TestCsvBackend(unittest.TestCase, CommonBackend):

    FILENAME = "test2.tmp"

    def _create(self):
        return CsvStorageBackend(self.FILENAME)

    def setUp(self):
        self.maxDiff = None
        self.backend = self._create()

    def tearDown(self):
        self.backend.unload()
        os.unlink(self.backend._get_filename())

    def test_format(self):
        # check if trailing zero's are removed.
        self.assertEqual(b"1,2,3", self.backend._format_row(1, 2, 3))

        self.assertEqual(b"1.1,2.2,3.3",
                         self.backend._format_row(1.1, 2.2, 3.3))

        # check if digits are rounded to 4 for timestamp, 8 for others
        self.assertEqual(b"0.1235,3.14159265,2.71828183",
                         self.backend._format_row(0.123456789, pi, e))

        # large numbers, do not want scientific notation
        self.assertEqual(b"2718281828.459,2718281828.45904493,2718281828.45904493",
                         self.backend._format_row(10**9*e, 10**9*e, 10**9*e))

