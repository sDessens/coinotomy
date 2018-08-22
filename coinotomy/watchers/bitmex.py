import urllib.request
import json
from dateutil import parser
import datetime

from coinotomy.watchers.common import Watcher
from coinotomy.utils.ticketsystem import TicketSystem

NORMAL_TIMEOUT = 60
FAST_TIMEOUT = 1

# maximum trades received per call call
MAX_TRADES = 500
MANY_TRADES = 200

EPOCH = 1483228800

class WatcherBitmex(Watcher):
    ticket_system = TicketSystem(1.01)

    def __init__(self, name: str, symbol: str):
        Watcher.__init__(self, "bitmex." + name, NORMAL_TIMEOUT)

        self.backend = None
        self.api = BitmexAPI(symbol, self.log)
        self.interval = FAST_TIMEOUT
        self.newest_ts = 0
        self.newest_tid = None

    def setup(self, backend):
        self.backend = backend

        self.newest_ts = EPOCH + 0.002
        for line in self.backend.rlines():
            self.newest_ts = line[0]
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


class BitmexAPI(object):
    def __init__(self, symbol, log):
        self.symbol = symbol
        self.log = log

    URL_SINCE_TS = \
        'https://www.bitmex.com/api/v1/trade?symbol={symbol}&startTime={start}&count={count}'
    URL_SINCE_TID = \
        'https://www.bitmex.com/api/v1/trade?symbol={symbol}&start={start}&count={count}'


    def more_since_ts_and_tid(self, since_ts, since_tid):
        url = self.URL_SINCE_TS.format(symbol=self.symbol,
                                       start=self.format_ts(since_ts),
                                       count=MAX_TRADES)
        html = self._query(url)
        return self._parse_response(html, since_ts, since_tid)

    def format_ts(self, since_ts):
        return datetime.datetime.utcfromtimestamp(since_ts).strftime('%Y-%m-%dT%H:%M:%S.%fZ')

    def parse_date(self, date):
        return parser.parse(date).timestamp()

    def _query(self, url):
        request = urllib.request.Request(url)
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
            tid = row['trdMatchID']
            ts = self.parse_date(row['timestamp'])
            price = float(row['price'])
            amount = float(row['size'])

            if found_tid:
                trades.append((ts, price, amount))
                since_tid = tid

            if since_tid == tid:
                found_tid = True

            since_ts = max(since_ts, ts)

        return trades, since_ts, since_tid

watchers = [
    WatcherBitmex("btc_usd", "XBTUSD"),
]

