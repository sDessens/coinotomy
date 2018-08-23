import urllib.request
import json
from dateutil import parser
import datetime

from coinotomy.watchers.common import Watcher
from coinotomy.utils.ticketsystem import TicketSystem

NORMAL_TIMEOUT = 60
FAST_TIMEOUT = 1

# maximum trades received per call call
MAX_TRADES = 1000
MANY_TRADES = 500

EPOCH = 1483228800

class WatcherQuoine(Watcher):
    ticket_system = TicketSystem(1.01)

    def __init__(self, name: str, symbol: str):
        Watcher.__init__(self, "quoine." + name, NORMAL_TIMEOUT)

        self.backend = None
        self.api = QuoineAPI(symbol, self.log)
        self.interval = FAST_TIMEOUT
        self.newest_ts = 0
        self.newest_tid = None

    def setup(self, backend):
        self.backend = backend

        self.newest_ts = EPOCH
        for line in self.backend.rlines():
            self.newest_ts = line[0] + 1
            break

    def tick(self):
        self.interval = NORMAL_TIMEOUT
        trades, self.newest_ts, self.newest_tid = self.api.more_since_ts_and_tid(self.newest_ts, self.newest_tid)

        for ts, p, v in trades:
            self.backend.append(ts, p, v)

        if len(trades) >= MANY_TRADES:
            self.interval = FAST_TIMEOUT

    def unload(self):
        if self.backend:
            self.backend.unload()
        self.backend = None

    def wait(self, first):
        self.ticket_system.get_ticket(self.interval)


class QuoineAPI(object):
    def __init__(self, symbol, log):
        self.symbol = symbol
        self.log = log

    URL_SINCE_TS = \
        'https://api.quoine.com/executions?currency_pair_code={product_id}&timestamp={ts}&limit={count}'
    HEADERS = {
        'User-Agent': 'python',
    }

    def more_since_ts_and_tid(self, since_ts, since_tid):
        url = self.URL_SINCE_TS.format(product_id=self.symbol,
                                       ts=int(since_ts),
                                       count=MAX_TRADES)
        html = self._query(url)
        return self._parse_response(html, since_ts, since_tid)

    def _query(self, url):
        request = urllib.request.Request(url, headers=self.HEADERS)
        with urllib.request.urlopen(request, timeout=10) as response:
            return str(response.read(), 'ascii')

    def _parse_response(self, html, since_ts, since_tid):
        """
        return array_of_trades
        """
        found_tid = since_tid is None
        js = json.loads(html)
        trades = []

        for row in js:
            tid = row['id']
            ts = float(row['created_at'])
            price = float(row['price'])
            amount = float(row['quantity'])

            if found_tid:
                trades.append((ts, price, amount))
                since_tid = tid

            if since_tid == tid:
                found_tid = True

            since_ts = max(since_ts, ts)

        return trades, since_ts, since_tid

watchers = [
    WatcherQuoine("btc_jpy", "BTCJPY"),
    WatcherQuoine("btc_usd", "BTCUSD"),
    WatcherQuoine("eth_jpy", "ETHJPY"),
    WatcherQuoine("eth_usd", "ETHUSD"),
    WatcherQuoine("eth_btc", "ETHBTC"),
    WatcherQuoine("bch_jpy", "BCHJPY"),
    WatcherQuoine("bch_usd", "BCHUSD"),

]

