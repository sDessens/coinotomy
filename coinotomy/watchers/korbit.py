import urllib.request
import json

from coinotomy.watchers.common import Watcher
import time

NORMAL_TIMEOUT = 60
FAST_TIMEOUT = 1


class WatcherKorbit(Watcher):
    def __init__(self, name: str, symbol: str):
        Watcher.__init__(self, "korbit." + name, FAST_TIMEOUT)

        self.backend = None
        self.api = KorbitAPI(symbol, self.log)

    def setup(self, backend):
        self.backend = backend

        # find the number of trades in the backend
        self.newest_tid = 0
        for trade in self.backend.lines():
            self.newest_tid += 1

    def tick(self):
        self.interval = NORMAL_TIMEOUT
        trades, self.newest_tid = self.api.more(self.newest_tid)


        for ts, p, v in trades:
            self.backend.append(ts, p, v)
        self.backend.flush()

        if len(trades) and trades[0][0] <= (time.time() - 60*60):
            self.interval = FAST_TIMEOUT

    def unload(self):
        if self.backend:
            self.backend.unload()
        self.backend = None


class KorbitAPI(object):
    HEADERS = {
        'User-Agent': 'python'
    }

    def __init__(self, symbol, log):
        self.symbol = symbol
        self.log = log

    def more(self, since_tid):
        return self._parse_response(self._query(since_tid), since_tid=since_tid)

    def _query(self, since_tid):
        url = 'https://api.korbit.co.kr/v1/transactions?currency_pair=%s&since=%i' % (self.symbol, since_tid)
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
        for row in js:
            tid = int(row['tid'])
            if tid <= since_tid:
                continue

            newest_tid = max(newest_tid, tid)
            timestamp = float(row['timestamp']) / 1000
            price = float(row['price'])
            amount = float(row['amount'])
            trades.append((timestamp, price, amount))

        return sorted(trades, key=lambda x: x[0]), newest_tid


watchers = [
    WatcherKorbit("btc_krw", "btc_krw"),
    WatcherKorbit("etc_krw", "etc_krw"),
    WatcherKorbit("eth_krw", "eth_krw"),
]