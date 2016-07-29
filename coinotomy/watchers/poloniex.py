import urllib.request
import json
from dateutil import parser
import time

from coinotomy.watchers.common import Watcher
from coinotomy.utils.ticketsystem import TicketSystem

NORMAL_TIMEOUT = 40
FAST_TIMEOUT = 1

INIT_WINDOW_SIZE = 60 * 60 * 24 * 30
MAX_TRADES = 50000


class WatcherPoloniex(Watcher):
    ticket_system = TicketSystem(1)

    ORIGIN = 1388534400  # 01 Jan 2014 00:00:00 GMT

    def __init__(self, name: str, symbol: str):
        Watcher.__init__(self, "poloniex." + name, NORMAL_TIMEOUT)

        self.backend = None
        self.api = PoloniexAPI(symbol, self.log, INIT_WINDOW_SIZE)
        self.newest_timestamp = self.ORIGIN
        self.interval = FAST_TIMEOUT

    def setup(self, backend):
        self.backend = backend

        # determine the timestamp of the last trade
        self.newest_timestamp = self.ORIGIN
        for trade in self.backend.rlines():
            self.newest_timestamp = trade[0]
            break

    def tick(self):
        trades = self.api.more_since_ts(self.newest_timestamp)

        # since the polo api returns the most recent 50000 trades, rather than the oldest 50000,
        # we will skip trades if we receive 50000 trades.
        # if that happens, decrease the window size and try again.
        if len(trades) == MAX_TRADES:
            self.api.window_size /= 2
            self.interval = FAST_TIMEOUT
            return

        if (time.time() - self.newest_timestamp) > self.api.window_size:
            # if the newest timestamp is not anywhere near the current timestamp,
            # we should just try again with updated timestamp
            if len(trades):
                self.newest_timestamp = trades[-1][0]
            else:
                self.newest_timestamp += self.api.window_size
            self.interval = FAST_TIMEOUT
        else:
            # if we received no trades, we probably caught up.
            if len(trades):
                self.newest_timestamp = trades[-1][0]
            self.interval = NORMAL_TIMEOUT

        for ts, p, v in trades:
            self.backend.append(ts, p, v)

    def unload(self):
        if self.backend:
            self.backend.unload()
        self.backend = None

    def wait(self, first):
        self.ticket_system.get_ticket(self.interval)


class PoloniexAPI(object):
    def __init__(self, symbol, log, window_size):
        self.symbol = symbol
        self.log = log
        self.window_size = window_size

    URL = \
        'https://poloniex.com/public?command=returnTradeHistory&currencyPair={pair}&start={start}&end={end}'
    HEADERS = {
        'User-Agent': 'python'
    }

    def more_since_ts(self, since_ts):
        import datetime
        url = self.URL.format(pair=self.symbol,
                              start=since_ts + 1,
                              end=since_ts + self.window_size)
        html = self._query(url)
        return self._parse_response(html)

    def parse_date(self, date):
        return parser.parse(date + '+0000').timestamp()

    def _query(self, url):
        request = urllib.request.Request(url, headers=self.HEADERS)
        with urllib.request.urlopen(request, timeout=10) as response:
            return str(response.read(), 'ascii')

    def _parse_response(self, html):
        """
        return array_of_trades
        """
        js = json.loads(html)
        trades = []

        for row in js:
            ts = self.parse_date(row['date'])
            price = float(row['rate'])
            amount = float(row['amount'])
            trades.append((ts, price, amount))

        trades = sorted(trades[::-1])
        if trades:
            return trades
        else:
            return trades

watchers = [
    WatcherPoloniex("eth_btc", "BTC_ETH"),
    WatcherPoloniex("btc_usdt", "USDT_BTC"),
    WatcherPoloniex("eth_usdt", "USDT_ETH"),
    WatcherPoloniex("etc_btc", "BTC_ETC"),
    WatcherPoloniex("etc_eth", "ETH_ETC"),
]
