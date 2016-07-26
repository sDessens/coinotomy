import unittest
import logging

from coinotomy.backend.ramdiskbackend import RamdiskStorageBackend
from coinotomy.watchers.yunbi import WatcherYunbi, YunbiAPI


log = logging.getLogger("dummy")

def is_sorted(trades):
    return all(a[0] <= b[0] for a, b in zip(trades[:-1], trades[1:]))


class TestWatcherYunbi(unittest.TestCase):

    # an actual response of 150 trades

    SAMPLE_RESPONSE = \
    """
[{"id":9749783,"price":"30.0","volume":"0.5652","funds":"16.956","market":"ethcny","created_at":"2015-08-15T06:08:03Z","at":1439618883,"side":"up"},
{"id":9749786,"price":"30.0","volume":"0.0009","funds":"0.027","market":"ethcny","created_at":"2015-08-15T06:13:03Z","at":1439619183,"side":"up"},
{"id":9749787,"price":"10.0","volume":"0.0027","funds":"0.027","market":"ethcny","created_at":"2015-08-15T06:13:48Z","at":1439619228,"side":"down"},
{"id":9749789,"price":"10.0","volume":"0.0027","funds":"0.027","market":"ethcny","created_at":"2015-08-15T06:15:31Z","at":1439619331,"side":"up"},
{"id":9749960,"price":"15.0","volume":"10.0","funds":"150.0","market":"ethcny","created_at":"2015-08-15T06:18:51Z","at":1439619531,"side":"up"},
{"id":9750236,"price":"15.0","volume":"0.001","funds":"0.015","market":"ethcny","created_at":"2015-08-15T06:24:33Z","at":1439619873,"side":"up"},
{"id":9750254,"price":"15.0","volume":"0.001","funds":"0.015","market":"ethcny","created_at":"2015-08-15T06:24:59Z","at":1439619899,"side":"up"},
{"id":9750797,"price":"13.0","volume":"10.0","funds":"130.0","market":"ethcny","created_at":"2015-08-15T06:36:20Z","at":1439620580,"side":"down"},
{"id":9750798,"price":"15.0","volume":"4.0","funds":"60.0","market":"ethcny","created_at":"2015-08-15T06:36:20Z","at":1439620580,"side":"up"},
{"id":9751553,"price":"14.5","volume":"0.2801","funds":"4.06145","market":"ethcny","created_at":"2015-08-15T06:51:59Z","at":1439621519,"side":"down"}]
    """

    EXPECTED_PARSE = [(1439618883.0, 30.0, 0.5652),
                      (1439619183.0, 30.0, 0.0009),
                      (1439619228.0, 10.0, 0.0027),
                      (1439619331.0, 10.0, 0.0027),
                      (1439619531.0, 15.0, 10.0),
                      (1439619873.0, 15.0, 0.001),
                      (1439619899.0, 15.0, 0.001),
                      (1439620580.0, 13.0, 10.0),
                      (1439620580.0, 15.0, 4.0),
                      (1439621519.0, 14.5, 0.2801)]
    EXPECTED_NEWEST_TID = 9751553

    def setUp(self):
        self.backend = RamdiskStorageBackend()
        self.watcher = WatcherYunbi("eth/cny", "ethcny")
        self.api = YunbiAPI("ethcny", log)

    def tearDown(self):
        del self.api

    def test_parse(self):
        trades, newest_tid = self.api._parse_response(self.SAMPLE_RESPONSE)
        self.assert_(is_sorted(trades))
        self.assertEqual(trades, self.EXPECTED_PARSE)
        self.assertEqual(newest_tid, self.EXPECTED_NEWEST_TID)

    def test_network_since_ts_0(self):
        trades, newest_tid = self.api.more_since_tid(1000)
        self.assertEqual(trades[0],  (1439618883.0, 30.0, 0.5652))  # 15 Aug 2015 06:08:03 GMT
        self.assertEqual(trades[-1], (1450341148.0, 6.48, 83.08))  # 17 Dec 2015 08:32:28 GMT
        self.assertEqual(len(trades), 1000)
        self.assertEqual(newest_tid, 17879603)
        self.assert_(is_sorted(trades))

    def test_network_until_2016(self):
        trades, newest_tid = self.api.more_until_ts(1451606400)  # 01 Jan 2016 00:00:00 GMT
        self.assertEqual(trades[0], (1439618883.0, 30.0, 0.5652))  # 01 Jan 2015 14:56:51 GMT
        self.assertEqual(len(trades), 1) # only one trade
        self.assertEqual(newest_tid, 9749783)
        self.assert_(is_sorted(trades))

        # check if re-querying the api returns only newer trades
        trades_2, newest_tid_2 = self.api.more_since_tid(newest_tid)
        self.assertNotEqual(trades_2[0], trades[0])
