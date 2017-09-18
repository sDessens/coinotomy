import urllib.request
import json

from coinotomy.watchers.common import Watcher


class WatcherBitfinex(Watcher):
    def __init__(self, name: str, symbol:str):
        Watcher.__init__(self, "bitfinex." + name, 2*60)

        self.backend = None
        self.api = BitfinexAPI(symbol, self.log)

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

    def unload(self):
        if self.backend:
            self.backend.unload()
        self.backend = None


class BitfinexAPI(object):
    def __init__(self, symbol, log):
        self.symbol = symbol
        self.log = log

    def more(self, since_tid=0):
        return self._parse_response(self._query(), since_tid=since_tid)

    def _query(self):
        url = "https://api.bitfinex.com/v1/trades/%s" % self.symbol
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
            timestamp = float(row['timestamp'])
            price = float(row['price'])
            amount = float(row['amount'])
            trades.append((timestamp, price, amount))

        # trades not guaranteed to have ordered timestamps, so sort them.
        return sorted(trades, key=lambda x: x[0]), newest_tid


watchers = [
    WatcherBitfinex("btc_usd", "btcusd"),
    WatcherBitfinex("ltc_usd", "ltcusd"),
    WatcherBitfinex("ltc_btc", "ltcbtc"),
    WatcherBitfinex("eth_usd", "ethusd"),
    WatcherBitfinex("eth_btc", "ethbtc"),
    WatcherBitfinex("etc_btc", "etcbtc"),
    WatcherBitfinex("etc_usd", "etcusd"),
    WatcherBitfinex("rrt_usd", "rrtusd"),
    WatcherBitfinex("rrt_btc", "rrtbtc"),
    WatcherBitfinex("zec_usd", "zecusd"),
    WatcherBitfinex("zec_btc", "zecbtc"),
    WatcherBitfinex("xmr_usd", "xmrusd"),
    WatcherBitfinex("xmr_btc", "xmrbtc"),
    WatcherBitfinex("dsh_usd", "dshusd"),
    WatcherBitfinex("dsh_btc", "dshbtc"),
    WatcherBitfinex("bcc_btc", "bccbtc"),
    WatcherBitfinex("bcu_btc", "bcubtc"),
    WatcherBitfinex("bcc_usd", "bccusd"),
    WatcherBitfinex("bcu_usd", "bcuusd"),
    WatcherBitfinex("xrp_usd", "xrpusd"),
    WatcherBitfinex("xrp_btc", "xrpbtc"),
    WatcherBitfinex("iot_usd", "iotusd"),
    WatcherBitfinex("iot_btc", "iotbtc"),
    WatcherBitfinex("iot_eth", "ioteth"),
    WatcherBitfinex("eos_usd", "eosusd"),
    WatcherBitfinex("eos_btc", "eosbtc"),
    WatcherBitfinex("eos_eth", "eoseth"),
    WatcherBitfinex("san_usd", "sanusd"),
    WatcherBitfinex("san_btc", "sanbtc"),
    WatcherBitfinex("san_eth", "saneth"),
    WatcherBitfinex("omg_usd", "omgusd"),
    WatcherBitfinex("omg_btc", "omgbtc"),
    WatcherBitfinex("omg_eth", "omgeth"),
    WatcherBitfinex("bch_usd", "bchusd"),
    WatcherBitfinex("bch_btc", "bchbtc"),
    WatcherBitfinex("bch_eth", "bcheth"),
    WatcherBitfinex("neo_usd", "neousd"),
    WatcherBitfinex("neo_btc", "neobtc"),
    WatcherBitfinex("neo_eth", "neoeth"),
]
