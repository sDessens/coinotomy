import urllib.request
import json

from coinotomy.watchers.common import Watcher

NORMAL_TIMEOUT = 15

class WatcherCoinone(Watcher):
    def __init__(self, name: str, symbol: str):
        Watcher.__init__(self, "coinone." + name, NORMAL_TIMEOUT)

        self.backend = None
        self.api = CoinoneAPI(symbol, self.log)

    def setup(self, backend):
        self.backend = backend

        # find the last trade in the storage backend
        last_trade = None
        for trade in self.backend.rlines():
            last_trade = trade
            break

        # determine the timestamp of the last trade
        if last_trade is None:
            self.newest_ts = 0
        else:
            self.newest_ts = last_trade[0] + 0.0001

    def tick(self):
        trades, self.newest_ts = self.api.more(self.newest_ts)

        for ts, p, v in trades:
            self.backend.append(ts, p, v)
        self.backend.flush()

    def unload(self):
        if self.backend:
            self.backend.unload()
        self.backend = None


class CoinoneAPI(object):
    def __init__(self, symbol, log):
        self.symbol = symbol
        self.log = log

    def more(self, since_ts=0):
        return self._parse_response(self._query(), since_ts=since_ts)

    def _query(self):
        url = 'https://api.coinone.co.kr/trades?currency=%s&format=json' % self.symbol
        with urllib.request.urlopen(url, timeout=30) as response:
            return str(response.read(), 'ascii')

    def _parse_response(self, html, since_ts=0):
        """
        return (array_of_trades, newest_ts)
        """
        js = json.loads(html)
        trades = []
        for row in js['completeOrders']:
            timestamp = float(row['timestamp'])
            price = float(row['price'])
            amount = float(row['qty'])

            if timestamp <= since_ts:
                continue

            trades.append((timestamp, price, amount))

        if trades:
            since_ts = max([x[0] for x in trades])

        return sorted(trades, key=lambda x: x[0]), since_ts


watchers = [
    WatcherCoinone("btc_krw", "btc"),
    WatcherCoinone("eth_krw", "eth"),
    WatcherCoinone("etc_krw", "etc"),
]