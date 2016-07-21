import urllib.request
import json

from coinotomy.watchers.common import Watcher

MAX_LIMIT = 5000
NORMAL_TIMEOUT = 2*60
FAST_TIMEOUT = 0


class WatcherBtcc(Watcher):
    def __init__(self, name: str):
        Watcher.__init__(self, "btcc." + name, 2*60)

        self.backend = None
        self.api = BtccAPI(self.log)

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
            self.newest_timestamp = last_trade[0] - 5
            self.newest_tid = 0

    def tick(self):
        if self.newest_tid:
            # trades filtered by api
            trades, newest_tid = self.api.more_since_tid(self.newest_tid)
        else:
            # Don't know newest TID. filter trades based on timestamp
            trades, self.newest_tid = self.api.more_since_timestamp(self.newest_timestamp)
            trades = list(filter(lambda row: row[0] >= self.newest_timestamp, trades))

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


class BtccAPI(object):
    def __init__(self, log):
        self.log = log

    URL_SINCE_TIMESTAMP = "https://data.btcchina.com/data/historydata?since={since}&limit={limit}&sincetype=time"
    URL_SINCE_TID = "https://data.btcchina.com/data/historydata?since={since}&limit={limit}"



    def more_since_timestamp(self, since_timestamp):
        # if since timestamp is 0, the api actually returns the newest trades.
        # Fix this by requesting since 1 instead.
        if since_timestamp == 0:
            since_timestamp = 1
        url = self.URL_SINCE_TIMESTAMP.format(limit=MAX_LIMIT, since=since_timestamp)
        html = self._query(url)

        return self._parse_response(html, self._since_timestamp_filter(since_timestamp), 0)

    def more_since_tid(self, since_tid):
        # if since tid is 0, the api actually returns the newest trades.
        # Fix this by requesting since 1 instead.
        if since_tid == 0:
            since_tid = 1
        url = self.URL_SINCE_TID.format(limit=MAX_LIMIT, since=since_tid)
        html = self._query(url)
        return self._parse_response(html, self._since_tid_filter(since_tid), since_tid)

    def _since_timestamp_filter(self, since_timestamp):
        return lambda tid, ts: ts > since_timestamp

    def _since_tid_filter(self, since_tid):
        return lambda tid, ts: tid > since_tid

    def _query(self, url):
        with urllib.request.urlopen(url, timeout=10) as response:
            return str(response.read(), 'ascii')

    def _parse_response(self, html, filter, since_tid):
        """
        return (array_of_trades, newest_tid)
        """
        js = json.loads(html)
        trades = []
        newest_tid = since_tid
        for row in js:
            tid = int(row['tid'])
            newest_tid = max(newest_tid, tid)
            timestamp = float(row['date'])
            price = float(row['price'])
            amount = float(row['amount'])
            if filter(tid, timestamp):
                trades.append((timestamp, price, amount))

        return trades, newest_tid


watchers = [
    WatcherBtcc("btc_cny"),
]