import unittest
import logging

from coinotomy.backend.ramdiskbackend import RamdiskStorageBackend
from coinotomy.watchers.poloniex import WatcherPoloniex, PoloniexAPI


log = logging.getLogger("dummy")

def is_sorted(trades):
    return all(a[0] <= b[0] for a, b in zip(trades[:-1], trades[1:]))


class TestWatcherPoloniex(unittest.TestCase):

    WINDOW_SIZE = 60 * 60 * 24 * 31

    SAMPLE_RESPONSE = \
    """
[{"globalTradeID":14756422,"tradeID":59961,"date":"2016-01-31 23:49:31","type":"buy","rate":"367.33999995","amount":"0.05540619","total":"20.35290983"},
{"globalTradeID":14756414,"tradeID":59960,"date":"2016-01-31 23:49:16","type":"sell","rate":"369.60999999","amount":"0.06735181","total":"24.89390249"},
{"globalTradeID":14756413,"tradeID":59959,"date":"2016-01-31 23:49:11","type":"buy","rate":"369.60999999","amount":"0.20000000","total":"73.92199999"},
{"globalTradeID":14756412,"tradeID":59958,"date":"2016-01-31 23:49:11","type":"buy","rate":"367.33999998","amount":"0.06402355","total":"23.51841085"},
{"globalTradeID":14756411,"tradeID":59957,"date":"2016-01-31 23:49:11","type":"buy","rate":"367.33999998","amount":"0.03402633","total":"12.49923206"},
{"globalTradeID":14756410,"tradeID":59956,"date":"2016-01-31 23:49:11","type":"buy","rate":"367.33999998","amount":"0.05540619","total":"20.35290983"},
{"globalTradeID":14756409,"tradeID":59955,"date":"2016-01-31 23:49:11","type":"buy","rate":"367.33999997","amount":"0.05016794","total":"18.42869107"},
{"globalTradeID":14756408,"tradeID":59954,"date":"2016-01-31 23:49:11","type":"buy","rate":"367.33999997","amount":"0.26938929","total":"98.95746178"},
{"globalTradeID":14756407,"tradeID":59953,"date":"2016-01-31 23:49:11","type":"buy","rate":"367.33999997","amount":"0.04003097","total":"14.70497651"},
{"globalTradeID":14756406,"tradeID":59952,"date":"2016-01-31 23:49:11","type":"buy","rate":"367.33999997","amount":"0.04866168","total":"17.87538152"}]
    """

    EXPECTED_PARSE = [(1454284151.0, 367.33999997, 0.04003097),
                      (1454284151.0, 367.33999997, 0.04866168),
                      (1454284151.0, 367.33999997, 0.05016794),
                      (1454284151.0, 367.33999997, 0.26938929),
                      (1454284151.0, 367.33999998, 0.03402633),
                      (1454284151.0, 367.33999998, 0.05540619),
                      (1454284151.0, 367.33999998, 0.06402355),
                      (1454284151.0, 369.60999999, 0.2),
                      (1454284156.0, 369.60999999, 0.06735181),
                      (1454284171.0, 367.33999995, 0.05540619)]
    EXPECTED_NEWEST_TS = 1454284171.0

    def setUp(self):
        self.backend = RamdiskStorageBackend()
        self.watcher = WatcherPoloniex("btc_usdt", "USDT_BTC")
        self.api = PoloniexAPI("USDT_BTC", log, self.WINDOW_SIZE)

    def tearDown(self):
        del self.api

    def test_parse(self):
        trades = self.api._parse_response(self.SAMPLE_RESPONSE)
        self.assertEqual(trades, self.EXPECTED_PARSE)

    def test_network_since_ts_2016(self):
        trades= self.api.more_since_ts(1451606400)  # 1 jan 2016
        self.assert_(is_sorted(trades))
        self.assertEqual(trades[0], (1451606843.0, 428.26000202, 0.17877408))  # 01 Jan 2016 00:07:23 GMT
        self.assertEqual(trades[-1], (1454284171.0, 367.33999995, 0.05540619))  # 31 Jan 2016 23:49:31 GMT
        self.assertEqual(len(trades), 6208)

    def test_network_since_ts_0(self):
        trades = self.api.more_since_ts(0)
        self.assert_(is_sorted(trades))
        self.assertEqual(len(trades), 0)
