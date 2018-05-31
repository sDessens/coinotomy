import urllib.request
import json

from coinotomy.watchers.common import Watcher

DEFAULT_TIMEOUT = 3 * 60


class WatcherWex(Watcher):
    def __init__(self, name: str, symbol: str):
        Watcher.__init__(self, "wex." + name, DEFAULT_TIMEOUT)

        self.backend = None
        self.api = WexAPI(symbol, self.log)

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


class WexAPI(object):
    SMALL_TRADE_SIZE = 50
    LARGE_TRADE_SIZE = 500
    HUGE_TRADE_SIZE = 5000

    def __init__(self, symbol, log):
        self.symbol = symbol
        self.log = log

    def more(self, since_tid=0):
        if since_tid == 0:
            return self._parse_response(self._query(self.HUGE_TRADE_SIZE), since_tid=since_tid)

        # progressively try to query more trades
        trades, new_tid = self._parse_response(self._query(self.SMALL_TRADE_SIZE), since_tid=since_tid)
        if len(trades) == self.SMALL_TRADE_SIZE:
            return trades, new_tid
        trades, new_tid = self._parse_response(self._query(self.LARGE_TRADE_SIZE), since_tid=since_tid)
        if len(trades) == self.LARGE_TRADE_SIZE:
            return trades, new_tid
        return self._parse_response(self._query(self.HUGE_TRADE_SIZE), since_tid=since_tid)

    def _query(self, amount):
        url = 'https://wex.nz/api/3/trades/%s?limit=%i' % (self.symbol, amount)
        with urllib.request.urlopen(url, timeout=10) as response:
            return str(response.read(), 'ascii')

    def _parse_response(self, html, since_tid=0):
        """
        return (array_of_trades, newest_tid)
        """
        js = json.loads(html)
        trades = []
        newest_tid = since_tid
        for row in js[list(js.keys())[0]]:
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
    WatcherWex("btc_usd", "btc_usd"),
    WatcherWex("btc_rur", "btc_rur"),
    WatcherWex("btc_eur", "btc_eur"),
    WatcherWex("ltc_btc", "ltc_btc"),
    WatcherWex("ltc_usd", "ltc_usd"),
    WatcherWex("ltc_rur", "ltc_rur"),
    WatcherWex("ltc_eur", "ltc_eur"),
    WatcherWex("nmc_btc", "nmc_btc"),
    WatcherWex("nmc_usd", "nmc_usd"),
    WatcherWex("nvc_btc", "nvc_btc"),
    WatcherWex("nvc_usd", "nvc_usd"),
    WatcherWex("usd_rur", "usd_rur"),
    WatcherWex("eur_usd", "eur_usd"),
    WatcherWex("eur_rur", "eur_rur"),
    WatcherWex("ppc_btc", "ppc_btc"),
    WatcherWex("ppc_usd", "ppc_usd"),
    WatcherWex("dsh_btc", "dsh_btc"),
    WatcherWex("dsh_usd", "dsh_usd"),
    WatcherWex("dsh_rur", "dsh_rur"),
    WatcherWex("dsh_eur", "dsh_eur"),
    WatcherWex("dsh_ltc", "dsh_ltc"),
    WatcherWex("dsh_eth", "dsh_eth"),
    WatcherWex("eth_btc", "eth_btc"),
    WatcherWex("eth_usd", "eth_usd"),
    WatcherWex("eth_eur", "eth_eur"),
    WatcherWex("eth_ltc", "eth_ltc"),
    WatcherWex("eth_rur", "eth_rur"),
    WatcherWex("bch_usd", "bch_usd"),
    WatcherWex("bch_btc", "bch_btc"),
    WatcherWex("bch_rur", "bch_rur"),
    WatcherWex("bch_eur", "bch_eur"),
    WatcherWex("bch_ltc", "bch_ltc"),
    WatcherWex("bch_eth", "bch_eth"),
    WatcherWex("bch_dsh", "bch_dsh"),
    WatcherWex("zec_btc", "zec_btc"),
    WatcherWex("zec_usd", "zec_usd"),
    WatcherWex("usdet_usd", "usdet_usd"),
    WatcherWex("ruret_rur", "ruret_rur"),
    WatcherWex("euret_eur", "euret_eur"),
    WatcherWex("btcet_btc", "btcet_btc"),
    WatcherWex("ltcet_ltc", "ltcet_ltc"),
    WatcherWex("ethet_eth", "ethet_eth"),
    WatcherWex("nmcet_nmc", "nmcet_nmc"),
    WatcherWex("nvcet_nvc", "nvcet_nvc"),
    WatcherWex("ppcet_ppc", "ppcet_ppc"),
    WatcherWex("dshet_dsh", "dshet_dsh"),
    WatcherWex("bchet_bch", "bchet_bch"),
]