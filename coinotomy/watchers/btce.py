import urllib.request
import json

from coinotomy.watchers.common import Watcher


class WatcherBtce(Watcher):
    def __init__(self, name: str, symbol: str):
        Watcher.__init__(self, "btce." + name, 5*60)

        self.backend = None
        self.api = BtceAPI(symbol, self.log)

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


class BtceAPI(object):
    def __init__(self, symbol, log):
        self.symbol = symbol
        self.log = log

    def more(self, since_tid=0):
        return self._parse_response(self._query(), since_tid=since_tid)

    def _query(self):
        url = 'https://btc-e.com/api/2/%s/trades/2000' % self.symbol
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
    WatcherBtce("btc_usd", "btc_usd"),
    WatcherBtce("eur_usd", "eur_usd"),
    WatcherBtce("ltc_usd", "ltc_usd"),
    WatcherBtce("eth_usd", "eth_usd"),
    WatcherBtce("nmc_usd", "nmc_usd"),
    WatcherBtce("nvc_usd", "nvc_usd"),
    WatcherBtce("ppc_usd", "ppc_usd"),
    WatcherBtce("btc_eur", "btc_eur"),
    WatcherBtce("ltc_eur", "ltc_eur"),
    WatcherBtce("ltc_btc", "ltc_btc"),
    WatcherBtce("dsh_btc", "dsh_btc"),
    WatcherBtce("eth_btc", "eth_btc"),
    WatcherBtce("nmc_btc", "nmc_btc"),
    WatcherBtce("nvc_btc", "nvc_btc"),
    WatcherBtce("ppc_btc", "ppc_btc"),
    WatcherBtce("eth_ltc", "eth_ltc"),
    WatcherBtce("usd_rur", "usd_rur"),
    WatcherBtce("eur_rur", "eur_rur"),
    WatcherBtce("btc_rur", "btc_rur"),
    WatcherBtce("ltc_rur", "ltc_rur"),
    WatcherBtce("dsh_usd", "dsh_usd"),
    WatcherBtce("eth_eur", "eth_eur"),
    WatcherBtce("eth_rur", "eth_rur")
]