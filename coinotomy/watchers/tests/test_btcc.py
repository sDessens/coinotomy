import unittest
import logging

from coinotomy.backend.ramdiskbackend import RamdiskStorageBackend
from coinotomy.watchers.btcc import WatcherBtcc, BtccAPI

log = logging.getLogger("dummy")


def is_sorted(trades):
    return all(a[0] <= b[0] for a, b in zip(trades[:-1], trades[1:]))


class TestWatcherBtcc(unittest.TestCase):

    # an actual response of 150 trades

    SAMPLE_RESPONSE = \
    """[{"date": "1311648637", "price": 89.01, "amount": 0.0004, "tid": "1001", "type": "buy"},
        {"date": "1311649555", "price": 87.94, "amount": 0.0067, "tid": "1002", "type": "sell"},
        {"date": "1311649804", "price": 87.94, "amount": 0.2933, "tid": "1003", "type": "buy"},
        {"date": "1311649804", "price": 89.01, "amount": 0.7067, "tid": "1004", "type": "buy"},
        {"date": "1311651325", "price": 89.01, "amount": 0.0306, "tid": "1005", "type": "buy"},
        {"date": "1311651325", "price": 89.99, "amount": 2, "tid": "1006", "type": "buy"},
        {"date": "1311652474", "price": 89.99, "amount": 0.0004, "tid": "1007", "type": "sell"},
        {"date": "1311655700", "price": 89.88, "amount": 3, "tid": "1008", "type": "buy"},
        {"date": "1311658264", "price": 89, "amount": 1, "tid": "1009", "type": "sell"},
        {"date": "1311659016", "price": 88.22, "amount": 1, "tid": "1010", "type": "sell"},
        {"date": "1311659923", "price": 88.3, "amount": 1.12, "tid": "1011", "type": "sell"},
        {"date": "1311659988", "price": 88.11, "amount": 1, "tid": "1012", "type": "sell"},
        {"date": "1311660301", "price": 88.1, "amount": 1.0795, "tid": "1013", "type": "sell"},
        {"date": "1311662348", "price": 88.1, "amount": 0.5, "tid": "1014", "type": "sell"},
        {"date": "1311665694", "price": 88.1, "amount": 1.0295, "tid": "1015", "type": "sell"},
        {"date": "1311665869", "price": 88.1, "amount": 0.245, "tid": "1016", "type": "sell"},
        {"date": "1311666826", "price": 88.76, "amount": 0.01, "tid": "1017", "type": "buy"},
        {"date": "1311666834", "price": 88.1, "amount": 6, "tid": "1018", "type": "sell"},
        {"date": "1311667086", "price": 88.76, "amount": 0.02, "tid": "1019", "type": "buy"},
        {"date": "1311667458", "price": 88.76, "amount": 1, "tid": "1020", "type": "buy"}]"""

    EXPECTED_SINCE_TID_1015 = ([(1311665869.0, 88.1, 0.245), (1311666826.0, 88.76, 0.01), (1311666834.0, 88.1, 6.0), (1311667086.0, 88.76, 0.02), (1311667458.0, 88.76, 1.0)], 1020)

    EXPECTED_SINCE_TIMESTAMP_1311659923 = ([(1311659988.0, 88.11, 1.0), (1311660301.0, 88.1, 1.0795), (1311662348.0, 88.1, 0.5), (1311665694.0, 88.1, 1.0295), (1311665869.0, 88.1, 0.245), (1311666826.0, 88.76, 0.01), (1311666834.0, 88.1, 6.0), (1311667086.0, 88.76, 0.02), (1311667458.0, 88.76, 1.0)], 1020)


    def setUp(self):
        self.backend = RamdiskStorageBackend()
        self.watcher = WatcherBtcc("btc_cny")
        self.api = BtccAPI(log)

    def tearDown(self):
        del self.api

    def test_network_since_tid_0(self):
        trades, newest_tid = self.api.more_since_tid(0)
        self.assertEqual(len(trades), 5000)
        self.assertIsInstance(newest_tid, int)
        self.assertEqual(newest_tid, 5229)

    def test_network_since_tid_x(self):
        trades, newest_tid = self.api.more_since_tid(10000)
        self.assertEqual(len(trades), 5000)
        self.assertIsInstance(newest_tid, int)
        self.assertEqual(newest_tid, 15000)

    def test_network_since_timestamp_0(self):
        trades, newest_tid = self.api.more_since_timestamp(0)
        self.assertEqual(len(trades), 5000)
        self.assertIsInstance(newest_tid, int)
        self.assertEqual(newest_tid, 5228)

    def test_network_since_timestamp_x(self):
        trades, newest_tid = self.api.more_since_timestamp(1451610061)  # 2016/01/01
        self.assertEqual(len(trades), 5000)
        self.assertIsInstance(newest_tid, int)
        self.assertEqual(newest_tid, 49647688)
        # notice that timestamp[0] < timestamp[-1]
        self.assertEqual(trades[0],  (1451610074.0, 2813.01, 0.8593))
        self.assertEqual(trades[-1], (1451618670.0, 2823.97, 0.189))

    def test_filtering_tid(self):
        result = self.api._parse_response(self.SAMPLE_RESPONSE, self.api._since_tid_filter(1015), 0)
        self.assertEqual(self.EXPECTED_SINCE_TID_1015, result)

    def test_filtering_timestamp(self):
        result = self.api._parse_response(self.SAMPLE_RESPONSE, self.api._since_timestamp_filter(1311659923), 0)
        self.assertEqual(self.EXPECTED_SINCE_TIMESTAMP_1311659923, result)
