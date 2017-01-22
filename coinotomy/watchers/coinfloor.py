import urllib.request
import json

from coinotomy.watchers.common import Watcher

NORMAL_TIMEOUT = 60*5

class WatcherCoinfloor(Watcher):
    def __init__(self, name: str, symbol: str):
        Watcher.__init__(self, "coinfloor." + name, NORMAL_TIMEOUT)

        self.backend = None
        self.api = CoinfloorAPI(symbol, self.log)

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


class CoinfloorAPI(object):
    def __init__(self, symbol, log):
        self.symbol = symbol
        self.log = log

    def more(self, since_tid=0):
        return self._parse_response(self._query(), since_tid=since_tid)

    def _query(self):

        url = 'https://webapi.coinfloor.co.uk:8090/bist/%s/transactions/' % self.symbol
        with urllib.request.urlopen(url, timeout=10) as response:
            return str(response.read(), 'ascii')

    def _parse_response(self, html, since_tid=0):
        """
        return (array_of_trades, newest_tid)
        """
        js = json.loads(html)
        trades = []
        newest_tid = since_tid
        for row in js:
            tid = int(row['tid'])
            if tid <= since_tid:
                continue

            newest_tid = max(newest_tid, tid)
            timestamp = float(row['date'])
            price = float(row['price'])
            amount = float(row['amount'])
            trades.append((timestamp, price, amount))

        # trades not guaranteed to have ordered timestamps, so sort them.
        # trades normally in reverse order, reverse them first.
        return sorted(trades[::-1], key=lambda x: x[0]), newest_tid


watchers = [
    WatcherCoinfloor('xbt_eur', 'XBT/EUR'),
    WatcherCoinfloor('xbt_gbp', 'XBT/GBP'),
    WatcherCoinfloor('xbt_usd', 'XBT/USD'),
    WatcherCoinfloor('xbt_pln', 'XBT/PLN'),
]


