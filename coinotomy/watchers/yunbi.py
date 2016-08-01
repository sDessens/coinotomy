import urllib.request
import json

from coinotomy.watchers.common import Watcher

MAX_LIMIT = 1000
NORMAL_TIMEOUT = 2*60
FAST_TIMEOUT = 10


class WatcherYunbi(Watcher):
    def __init__(self, name: str, symbol: str):
        Watcher.__init__(self, "yunbi." + name, FAST_TIMEOUT)

        self.backend = None
        self.api = YunbiAPI(symbol, self.log)

        self.newest_tid = None
        self.newest_timestamp = None

    def setup(self, backend):
        self.backend = backend

        # find the last timestamp
        last_trade = None
        for trade in self.backend.lines():
            last_trade = trade

        # determine the timestamp of the last trade
        if last_trade is None:
            self.newest_timestamp = 1
            self.newest_tid = 1
        else:
            self.newest_timestamp = last_trade[0]
            self.newest_tid = None

    def tick(self):
        # if we don't have the newest td yet, do an request to find what the newest tid is
        # then wait for the next iteration
        if self.newest_tid is None:
            trades, self.newest_tid = self.api.more_until_ts(self.newest_timestamp)
            return
        else:
            trades, newest_tid = self.api.more_since_tid(self.newest_tid)
            if newest_tid is None:
                return
            self.newest_tid = newest_tid

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


class YunbiAPI(object):
    def __init__(self, symbol, log):
        self.symbol = symbol
        self.log = log

    URL_UNTIL_TS = "https://yunbi.com//api/v2/trades.json?market={symbol}&timestamp={ts}&order_by=desc&limit=1"
    URL_SINCE_TID = "https://yunbi.com//api/v2/trades.json?market={symbol}&from={tid}&order_by=asc&limit=1000"
    HEADERS = {
        'User-Agent': 'python',
        'Accept': 'application/json'
    }

    def more_since_tid(self, since_tid):
        url = self.URL_SINCE_TID.format(symbol=self.symbol, tid=max(since_tid, 1))
        html = self._query(url)
        return self._parse_response(html)

    def more_until_ts(self, until_ts):
        url = self.URL_UNTIL_TS.format(symbol=self.symbol, ts=until_ts)
        html = self._query(url)
        return self._parse_response(html)

    def _query(self, url):
        request = urllib.request.Request(url, headers=self.HEADERS)
        with urllib.request.urlopen(request, timeout=10) as response:
            return str(response.read(), 'ascii')

    def _parse_response(self, html):
        """
        return (array_of_trades, newest_tid)
        """

        js = json.loads(html)
        trades = []
        newest_tid = None
        for row in js:
            tid = int(row['id'])
            newest_tid = tid
            timestamp = float(row['at'])
            price = float(row['price'])
            amount = float(row['volume'])
            trades.append((timestamp, price, amount))

        return trades, newest_tid


watchers = [
    WatcherYunbi("btc_cny", "btccny"),
    WatcherYunbi("eth_cny", "ethcny"),
    WatcherYunbi("dgd_cny", "dgdcny"),
    WatcherYunbi("etc_cny", "etccny"),
    WatcherYunbi("dgd_btc", "dgdbtc"),
    WatcherYunbi("eth_btc", "ethbtc"),
    WatcherYunbi("pls_cny", "plscny"),
    WatcherYunbi("bts_cny", "btscny"),
    WatcherYunbi("bitcny_cny", "bitcnycny"),
    WatcherYunbi("dcs_cny", "dcscny"),
    WatcherYunbi("sc_cny", "sccny"),
    WatcherYunbi("dao_cny", "daocny"),
    WatcherYunbi("dao_btc", "daobtc"),
]