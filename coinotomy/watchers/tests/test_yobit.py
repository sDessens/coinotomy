import unittest
import logging

from coinotomy.backend.ramdiskbackend import RamdiskStorageBackend
from coinotomy.watchers.yobit import WatcherYobit, YobitAPI

log = logging.getLogger("dummy")


def is_sorted(trades):
    return all(a[0] <= b[0] for a, b in zip(trades[:-1], trades[1:]))


class TestWatcherYobit(unittest.TestCase):
    # an actual response of 10 trades
    RESPONSE_1 = '{"dash_btc":[{"type":"bid","price":0.02299804,"amount":57.166591,"tid":16283329,"timestamp":1471787192},{"type":"bid","price":0.02299866,"amount":80.772725,"tid":16283148,"timestamp":1471787002},{"type":"bid","price":0.0229,"amount":0.00563679,"tid":16283142,"timestamp":1471786994},{"type":"bid","price":0.02286636,"amount":12.5652194,"tid":16283093,"timestamp":1471786917},{"type":"bid","price":0.02286636,"amount":0.4347826,"tid":16283046,"timestamp":1471786886},{"type":"bid","price":0.02286635,"amount":0.90598928,"tid":16283031,"timestamp":1471786883},{"type":"bid","price":0.02268506,"amount":0.00291796,"tid":16282992,"timestamp":1471786865},{"type":"bid","price":0.02268352,"amount":32.332636,"tid":16282771,"timestamp":1471786796},{"type":"bid","price":0.02268398,"amount":39.186134,"tid":16282665,"timestamp":1471786611},{"type":"ask","price":0.02268506,"amount":0.00598928,"tid":16282656,"timestamp":1471786553}]}'

    RESPONSE_1_EXPECTED = [(1471786553.0, 0.02268506, 0.00598928),
                           (1471786611.0, 0.02268398, 39.186134),
                           (1471786796.0, 0.02268352, 32.332636),
                           (1471786865.0, 0.02268506, 0.00291796),
                           (1471786883.0, 0.02286635, 0.90598928),
                           (1471786886.0, 0.02286636, 0.4347826),
                           (1471786917.0, 0.02286636, 12.5652194),
                           (1471786994.0, 0.0229, 0.00563679),
                           (1471787002.0, 0.02299866, 80.772725),
                           (1471787192.0, 0.02299804, 57.166591)]

    # an actual response of 4 trades
    RESPONSE_2 = ''
    RESPONSE_2_EXPECTED = []

    def setUp(self):
        self.backend = RamdiskStorageBackend()
        self.watcher = WatcherYobit("dash_btc", "dash_btc")
        self.api = YobitAPI("dash_btc", log)

    def tearDown(self):
        del self.api

    def test_network(self):
        trades, newest_tid = self.api.more()
        self.assertEqual(len(trades), 2000)
        self.assertIsInstance(newest_tid, int)

    def test_parse_response(self):
        trades, newest_tid = self.api._parse_response(self.RESPONSE_1)
        self.assertEqual(newest_tid, 16283329)
        for row in trades:
            self.assertIsInstance(row[0], float)
            self.assertIsInstance(row[1], float)
            self.assertIsInstance(row[2], float)
        self.assert_(is_sorted(trades))
        self.assertEqual(len(trades), 10)
        self.assertEqual(trades, self.RESPONSE_1_EXPECTED)
