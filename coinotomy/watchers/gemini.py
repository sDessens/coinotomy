import json
import urllib.request
import time

from coinotomy.utils.ticketsystem import TicketSystem
from coinotomy.watchers.common import Watcher

NORMAL_TIMEOUT = 1
SLOW_TIMEOUT = 2*60

MAXIMUM_HISTORY_LOOKBACK = 60*60*24*6.8 # slightly less than 7 days


class WatcherGemini(Watcher):
    # share rate limiting between all hitbtc instances
    ticket_system = TicketSystem(NORMAL_TIMEOUT)

    def __init__(self, name, symbol):
        Watcher.__init__(self, "gemini." + name, 1)

        self.backend = None
        self.api = GeminiApi(symbol, self.log)

        self.newest_timestamp = 0

    def __del__(self):
        self.unload()

    def name(self):
        return self.name

    def setup(self, backend):
        self.backend = backend

        # find the last trade in the storage backend
        last_trade = None
        for trade in self.backend.rlines():
            last_trade = trade
            break

        # determine the timestamp of the last trade
        self.newest_tid = 0
        if last_trade is None:
            self.newest_timestamp = 0
        else:
            self.newest_timestamp = last_trade[0] + 0.001

    def tick(self):
        self.newest_timestamp = max(time.time() - MAXIMUM_HISTORY_LOOKBACK, self.newest_timestamp)

        self.interval = SLOW_TIMEOUT
        trades, self.newest_timestamp, self.newest_tid, fast_retry = \
            self.api.more_ts(self.newest_timestamp, self.newest_tid)

        for ts, p, v in trades:
            self.backend.append(ts, p, v)
        self.backend.flush()

        if fast_retry:
            self.interval = NORMAL_TIMEOUT

    def unload(self):
        if self.backend:
            self.backend.unload()
        self.backend = None

    def wait(self, first):
        self.ticket_system.get_ticket(self.interval)


class GeminiApi(object):
    MAX_TRADES = 500

    def __init__(self, symbol, log):
        self.symbol = symbol
        self.log = log

    def more_ts(self, since_ts, since_tid):
        return self._parse_response(self._query(since_ts), since_ts=since_ts, since_tid=since_tid)

    def _query(self, since_ts):
        url = 'https://api.gemini.com/v1/trades/{symbol}?timestamp={timestamp}&limit_trades={limit}' \
            .format(symbol=self.symbol, timestamp=int(since_ts * 1000), limit=self.MAX_TRADES)

        with urllib.request.urlopen(url, timeout=10) as response:
            return str(response.read(), 'ascii')

    def _parse_response(self, html, since_ts, since_tid):
        """
        return (array_of_trades, newest_ts, newest_tid, fast_retry)
        """
        js = json.loads(html)
        trades = []
        newest_ts = since_ts
        newest_tid = since_tid
        for row in js:
            tid = row['tid']
            ts = row['timestampms'] / 1000.0
            price = float(row['price'])
            amount = float(row['amount'])

            if ts <= since_ts or tid < since_tid:
                continue

            newest_ts = max(newest_ts, ts)
            newest_tid = max(newest_tid, ts)
            trades.append((ts, price, amount))

        return trades[::-1], newest_ts, since_tid, len(js) == self.MAX_TRADES


watchers = [
    WatcherGemini("btc_usd", "btcusd"),
    WatcherGemini("eth_btc", "ethbtc"),
    WatcherGemini("eth_usd", "ethusd"),
]
