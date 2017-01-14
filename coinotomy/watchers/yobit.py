import urllib.request
import json

from coinotomy.watchers.common import Watcher

MAX_LIMIT = 2000
TIMEOUT = 5 * 60

class WatcherYobit(Watcher):
    def __init__(self, name: str, symbol: str):
        Watcher.__init__(self, "yobit." + name, TIMEOUT)

        self.backend = None
        self.api = YobitAPI(symbol, self.log)

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


class YobitAPI(object):
    def __init__(self, symbol, log):
        self.symbol = symbol
        self.log = log

    HEADERS = {
        'User-Agent': 'python'
    }

    def more(self, since_tid=0):
        return self._parse_response(self._query(), since_tid=since_tid)

    def _query(self):
        url = 'https://yobit.net/api/3/trades/%s?limit=2000' % self.symbol
        request = urllib.request.Request(url, headers=self.HEADERS)
        with urllib.request.urlopen(request, timeout=10) as response:
            return str(response.read(), 'ascii')

    def _parse_response(self, html, since_tid=0):
        """
        return (array_of_trades, newest_tid)
        """
        js = json.loads(html)
        trades = []
        newest_tid = since_tid
        for row in js[self.symbol]:
            tid = int(row['tid'])
            if tid <= since_tid:
                continue

            newest_tid = max(newest_tid, tid)
            timestamp = float(row['timestamp'])
            price = float(row['price'])
            amount = float(row['amount'])
            trades.append((timestamp, price, amount))

        # trades not guaranteed to have ordered timestamps, so sort them.
        # trades normally in reverse order, reverse them first.
        return sorted(trades[::-1], key=lambda x: x[0]), newest_tid


watchers = [
    WatcherYobit("etc_btc", "etc_btc"),
    WatcherYobit("btc_usd", "btc_usd"),
    WatcherYobit("eth_btc", "eth_btc"),
    WatcherYobit("eth_usd", "eth_usd"),
    WatcherYobit("lsk_btc", "lsk_btc"),
    WatcherYobit("dsh_btc", "dash_btc"),
]

