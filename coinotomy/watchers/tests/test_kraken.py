import unittest
import logging

from coinotomy.backend.ramdiskbackend import RamdiskStorageBackend
from coinotomy.watchers.kraken import WatcherKraken, KrakenAPI


log = logging.getLogger("dummy")

def is_sorted(trades):
    return all(a[0] <= b[0] for a, b in zip(trades[:-1], trades[1:]))


class TestWatcherKraken(unittest.TestCase):

    # an actual response of 150 trades

    SAMPLE_RESPONSE = \
    """
{"error":
[],"result":{"XXBTZUSD":
[
["122.00000","0.10000000",1381095255.5514,"s","l",""],
["123.61000","0.10000000",1381179030.4815,"s","l",""],
["123.91000","1.00000000",1381201115.641,"s","l",""],
["123.90000","0.99160000",1381201115.6567,"s","l",""],
["124.19000","1.00000000",1381210004.1001,"s","l",""],
["124.18000","1.00000000",1381210004.1077,"s","l",""],
["124.01687","1.00000000",1381311039.011,"s","l",""],
["124.01687","1.00000000",1381311093.9123,"s","l",""],
["123.84000","0.82300000",1381311094.4288,"b","l",""],
["125.85000","1.00000000",1381431835.1776,"b","l",""]
],"last":"1383581942406609135"}}
    """

    EXPECTED_PARSE = [(1381095255.5514, 122.0, 0.1),
                      (1381179030.4815, 123.61, 0.1),
                      (1381201115.641, 123.91, 1.0),
                      (1381201115.6567, 123.9, 0.9916),
                      (1381210004.1001, 124.19, 1.0),
                      (1381210004.1077, 124.18, 1.0),
                      (1381311039.011, 124.01687, 1.0),
                      (1381311093.9123, 124.01687, 1.0),
                      (1381311094.4288, 123.84, 0.823),
                      (1381431835.1776, 125.85, 1.0)]
    EXPECTED_NEWEST_TS = 1383581942.406609

    def setUp(self):
        self.backend = RamdiskStorageBackend()
        self.watcher = WatcherKraken("btc_usd", "XXBTZUSD")
        self.api = KrakenAPI("XXBTZUSD", log)

    def tearDown(self):
        del self.api

    def test_parse(self):
        trades, newest_ts = self.api._parse_response(self.SAMPLE_RESPONSE)
        self.assertEqual(trades, self.EXPECTED_PARSE)
        self.assertEqual(newest_ts, self.EXPECTED_NEWEST_TS)

    def test_network_since_ts_0(self):
        trades, newest_ts = self.api.more_since_ts(0)
        self.assertEqual(trades[0], (1381095255.5514, 122.0, 0.1))  # 06 Oct 2013 21:34:15 GMT
        self.assertEqual(trades[-1], (1383581942.4066, 220.36266, 0.11850777))  # 04 Nov 2013 16:19:02 GMT
        self.assertEqual(len(trades), 1000)
        self.assertEqual(newest_ts, 1383581942.406609)
        self.assert_(is_sorted(trades))

    def test_network_since_2015(self):
        trades, newest_ts = self.api.more_since_ts(1420070400)  # 01 jan 2015 00:00:00 GMT
        self.assertEqual(trades[0], (1420124211.8014, 314.02868, 0.22))  # 01 Jan 2015 14:56:51 GMT
        self.assertEqual(trades[-1], (1421112636.0729, 256.62747, 0.1))  # 13 Jan 2015 01:30:36 GMT
        self.assertEqual(len(trades), 1000)
        self.assertEqual(newest_ts, 1421112636.0728729)
        self.assert_(is_sorted(trades))
