import urllib.request
import json

from coinotomy.watchers.common import Watcher

MAX_LIMIT = 1000
NORMAL_TIMEOUT = 2*60
FAST_TIMEOUT = 5


class WatcherCexio(Watcher):
    def __init__(self, name: str, symbol: str):
        Watcher.__init__(self, "cexio." + name, NORMAL_TIMEOUT)

        self.backend = None
        self.api = CexioAPI(symbol, self.log)

    def setup(self, backend):
        self.backend = backend

        # count the amount of trades in the backend.
        # and find the last timestamp
        last_trade = None
        n_trades = 0
        for trade in self.backend.lines():
            last_trade = trade
            n_trades += 1

        print(n_trades, last_trade)

        # determine the timestamp of the last trade
        if last_trade is None:
            self.newest_timestamp = 0
            self.newest_tid = 0
        else:
            # the newest timestamp is only meant to prevent double-inclusion of trades,
            # we don't need to update it after it is set once
            self.newest_timestamp = last_trade[0]
            # cex api works by tid, but we don't store the last tic
            # guess that the last tid is equal to the amount of trades in the file
            # then do additional filtering in tick() with the nestest_timestamp
            self.newest_tid = n_trades

    def tick(self):
        # trades filtered by api
        trades, self.newest_tid = self.api.more_since_tid(self.newest_tid)
        trades = list(filter(lambda row: row[0] > self.newest_timestamp, trades))

        for ts, p, v in trades:
            self.backend.append(ts, p, v)

        if len(trades) == MAX_LIMIT:
            self.interval = FAST_TIMEOUT
        else:
            self.interval = NORMAL_TIMEOUT

    def unload(self):
        if self.backend:
            self.backend.unload()
        self.backend = None


class CexioAPI(object):
    def __init__(self, symbol, log):
        self.symbol = symbol
        self.log = log

    URL_SINCE_TID = "https://cex.io/api/trade_history/{symbol}/?since={since}"
    HEADERS = {
        'User-Agent': 'python'
    }

    def more_since_tid(self, since_tid):
        # if since tid is 0, the api actually returns the newest trades.
        # Fix this by requesting since 1 instead.
        url = self.URL_SINCE_TID.format(symbol=self.symbol, since=max(since_tid, 1))
        html = self._query(url)
        return self._parse_response(html, self._since_tid_filter(since_tid))

    def _since_tid_filter(self, since_tid):
        return lambda tid, ts: tid >= since_tid

    def _query(self, url):
        request = urllib.request.Request(url, headers=self.HEADERS)
        with urllib.request.urlopen(request, timeout=10) as response:
            return str(response.read(), 'ascii')

    def _parse_response(self, html, filter):
        """
        return (array_of_trades, newest_tid)
        """
        js = json.loads(html)
        trades = []
        newest_tid = 0
        for row in js:
            tid = int(row['tid'])
            newest_tid = max(newest_tid, tid + 1)
            timestamp = float(row['date'])
            price = float(row['price'])
            amount = float(row['amount'])
            if filter(tid, timestamp):
                trades.append((timestamp, price, amount))
            else:
                print(row)

        return trades[::-1], newest_tid


watchers = [
    WatcherCexio("btc_usd", "BTC/USD"),
    WatcherCexio("btc_eur", "BTC/EUR"),
    WatcherCexio("ghs_btc", "GHS/BTC"),
    WatcherCexio("ltc_usd", "LTC/USD"),
    WatcherCexio("ltc_btc", "LTC/BTC"),
    WatcherCexio("ltc_eur", "LTC/EUR"),
    WatcherCexio("btc_rub", "BTC/RUB"),
    WatcherCexio("eth_btc", "ETH/BTC"),
    WatcherCexio("eth_usd", "ETH/USD"),
    WatcherCexio("eth_eur", "ETH/EUR"),
]