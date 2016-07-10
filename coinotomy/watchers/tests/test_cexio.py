import unittest
import logging

from coinotomy.backend.ramdiskbackend import RamdiskStorageBackend
from coinotomy.watchers.cexio import WatcherCexio, CexioAPI


log = logging.getLogger("dummy")

def is_sorted(trades):
    return all(a[0] <= b[0] for a, b in zip(trades[:-1], trades[1:]))


class TestWatcherCexio(unittest.TestCase):

    # an actual response of 150 trades

    SAMPLE_RESPONSE = \
    """
[{"type":"buy","date":"1405872964","amount":"0.05985928","price":"613.00000000","tid":"1000"},
{"type":"buy","date":"1405872964","amount":"0.00064442","price":"612.99200000","tid":"999"},
{"type":"buy","date":"1405872964","amount":"0.00540000","price":"612.98000000","tid":"998"},
{"type":"buy","date":"1405871309","amount":"0.00060358","price":"613.00000000","tid":"997"},
{"type":"sell","date":"1405871276","amount":"0.00021914","price":"608.10000000","tid":"996"},
{"type":"sell","date":"1405871276","amount":"0.00042383","price":"612.90000000","tid":"995"},
{"type":"buy","date":"1405871060","amount":"0.00052053","price":"612.90000000","tid":"994"},
{"type":"buy","date":"1405870909","amount":"0.00017947","price":"612.90000000","tid":"993"},
{"type":"sell","date":"1405869650","amount":"0.00088802","price":"608.10000000","tid":"992"}]
    """

    EXPECTED_SINCE_TID_995 = ([(1405871276.0, 612.9, 0.00042383),
                               (1405871276.0, 608.1, 0.00021914),
                               (1405871309.0, 613.0, 0.00060358),
                               (1405872964.0, 612.98, 0.0054),
                               (1405872964.0, 612.992, 0.00064442),
                               (1405872964.0, 613.0, 0.05985928)], 1001)

    def setUp(self):
        self.backend = RamdiskStorageBackend()
        self.watcher = WatcherCexio("btc_usd", "BTC/USD")
        self.api = CexioAPI("BTC/USD", log)

    def tearDown(self):
        del self.api

    def test_network_since_tid_0(self):
        trades, newest_tid = self.api.more_since_tid(0)
        self.assertEqual(len(trades), 1000)
        self.assertIsInstance(newest_tid, int)
        self.assertEqual(newest_tid, 1001)
        self.assert_(is_sorted(trades))

    def test_network_since_tid_x(self):
        trades, newest_tid = self.api.more_since_tid(1000)
        print(trades, newest_tid)
        self.assertEqual(len(trades), 1000)
        self.assertIsInstance(newest_tid, int)
        self.assertEqual(newest_tid, 2000)
        self.assert_(is_sorted(trades))

    def test_filtering_tid(self):
        result = self.api._parse_response(self.SAMPLE_RESPONSE, self.api._since_tid_filter(995))
        self.assertEqual(self.EXPECTED_SINCE_TID_995, result)
