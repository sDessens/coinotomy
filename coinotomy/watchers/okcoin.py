import json
import requests

from coinotomy.watchers.common import Watcher

NORMAL_TIMEOUT = 1

TYPE_CN = 1
TYPE_INT = 2

class WatcherOkcoin(Watcher):
    def __init__(self, name: str, type, symbol: str):
        Watcher.__init__(self, "okcoin." + name, NORMAL_TIMEOUT)

        self.backend = None
        self.api = OkcoinAPI(symbol, type, self.log)

    def setup(self, backend):
        self.backend = backend

        # find the last trade in the storage backend
        last_trade = None
        for trade in self.backend.rlines():
            last_trade = trade
            break

        # determine the timestamp of the last trade
        if last_trade is None:
            self.newest_timestamp = 0
            self.newest_tid = 0
        else:
            self.newest_timestamp = last_trade[0] + 0.0001
            self.newest_tid = 0

    def tick(self):
        if self.newest_tid:
            # trades filtered by api
            trades, self.newest_tid = self.api.more(self.newest_tid)
        else:
            # Don't know newest TID. filter trades based on timestamp
            trades, self.newest_tid = self.api.more()
            trades = list(filter(lambda row: row[0] >= self.newest_timestamp, trades))

        for ts, p, v in trades:
            self.backend.append(ts, p, v)
        self.backend.flush()

    def unload(self):
        if self.backend:
            self.backend.unload()
        self.backend = None


class OkcoinAPI(object):


    def __init__(self, symbol, type, log):
        self.symbol = symbol
        self.type = type
        self.log = log

    def more(self, since_tid=0):
        return self._parse_response(self._query(since_tid), since_tid=since_tid)

    def _query(self, since_tid):
        if since_tid == 0:
            since_tid = 1
        if self.type == TYPE_CN:
            url = 'https://www.okcoin.cn/api/v1/trades.do?since=%i&symbol=%s' % (since_tid, self.symbol)
        else:
            url = 'https://www.okcoin.com/api/v1/trades.do?since=%i&symbol=%s' % (since_tid, self.symbol)

        return requests.get(url, timeout=10).json()

    def _parse_response(self, js, since_tid=0):
        """
        return (array_of_trades, newest_tid)
        """
        trades = []
        newest_tid = since_tid
        for row in js:
            tid = int(row['tid'])
            if tid <= since_tid:
                continue

            newest_tid = max(newest_tid, tid)
            timestamp = float(row['date_ms']) / 1000.0
            price = float(row['price'])
            amount = float(row['amount'])
            trades.append((timestamp, price, amount))

        return sorted(trades, key=lambda x: x[0]), newest_tid


watchers = [
    WatcherOkcoin("btc_cny", TYPE_CN, "btc_cny"),
    WatcherOkcoin("ltc_cny", TYPE_CN, "ltc_cny"),

    WatcherOkcoin("btc_usd", TYPE_INT, "btc_usd"),
    WatcherOkcoin("ltc_usd", TYPE_INT, "ltc_usd"),
]