import urllib.request
import json
from dateutil import parser

from coinotomy.watchers.common import Watcher


class WatcherGdax(Watcher):
    def __init__(self, name: str, symbol:str):
        Watcher.__init__(self, "gdax." + name, 2*60)

        self.backend = None
        self.api = GdaxAPI(symbol, self.log)

    def setup(self, backend):
        self.backend = backend

        # determine the last tid, which is just the number of trades.
        self.last_tid = sum(1 for _ in self.backend.lines())

    def tick(self):
        # trades filtered by api
        trades, self.last_tid = self.api.more(self.last_tid)

        for ts, p, v in trades:
            self.backend.append(ts, p, v)

        if len(trades) == self.api.MAX_TRADES:
            self.interval = 1
        else:
            self.interval = 60

    def unload(self):
        if self.backend:
            self.backend.unload()
        self.backend = None


class GdaxAPI(object):
    MAX_TRADES = 100

    def __init__(self, symbol, log):
        self.symbol = symbol
        self.log = log

    def parse_date(self, date):
        return parser.parse(date).timestamp()

    def more(self, since_tid):
        return self._parse_response(self._query(since_tid + self.MAX_TRADES + 1), since_tid)

    def _query(self, after):
        # "after" actually returns trades with a lower tid
        url = "https://api.gdax.com/products/%s/trades?after=%s" % (self.symbol, after)
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
            tid = int(row['trade_id'])
            if tid <= since_tid:
                continue

            newest_tid = max(newest_tid, tid)
            timestamp = self.parse_date(row['time'])
            price = float(row['price'])
            amount = float(row['size'])
            trades.append((timestamp, price, amount))

        # trades not guaranteed to have ordered timestamps, so sort them.
        return sorted(trades, key=lambda x: x[0]), newest_tid

watchers = [
    WatcherGdax('btc_gbp', 'BTC-GBP'),
    WatcherGdax('btc_eur', 'BTC-EUR'),
    WatcherGdax('eth_usd', 'ETH-USD'),
    WatcherGdax('eth_btc', 'ETH-BTC'),
    WatcherGdax('ltc_usd', 'LTC-USD'),
    WatcherGdax('ltc_btc', 'LTC-BTC'),
    WatcherGdax('btc_usd', 'BTC-USD'),
]

