import urllib.request
import json

from coinotomy.watchers.common import Watcher
from coinotomy.utils.ticketsystem import TicketSystem

MAX_LIMIT = 1000
NORMAL_TIMEOUT = 20
SLOW_TIMEOUT = 1000

class WatcherKraken(Watcher):
    # share rate limiting between all kraken instances
    ticket_system = TicketSystem(NORMAL_TIMEOUT)

    def __init__(self, name: str, symbol: str):
        Watcher.__init__(self, "kraken." + name, NORMAL_TIMEOUT)

        self.backend = None
        self.api = KrakenAPI(symbol, self.log)
        self.newest_timestamp = 0

    def setup(self, backend):
        self.backend = backend

        # determine the timestamp of the last trade
        self.newest_timestamp = 0
        for trade in self.backend.rlines():
            self.newest_timestamp = trade[0]
            break

    def tick(self):
        # trades filtered by api
        trades, self.newest_timestamp = self.api.more_since_ts(self.newest_timestamp)

        for ts, p, v in trades:
            self.backend.append(ts, p, v)

        if len(trades) == MAX_LIMIT:
            self.interval = NORMAL_TIMEOUT
        else:
            self.interval = SLOW_TIMEOUT

    def unload(self):
        if self.backend:
            self.backend.unload()
        self.backend = None

    def wait(self, first):
        self.ticket_system.get_ticket(self.interval)


class KrakenAPI(object):
    def __init__(self, symbol, log):
        self.symbol = symbol
        self.log = log

    URL_SINCE_TS = "https://api.kraken.com/0/public/Trades?pair={pair}&since={since}"
    HEADERS = {
        'User-Agent': 'python'
    }

    def more_since_ts(self, since_ts):
        url = self.URL_SINCE_TS.format(pair=self.symbol,
                                       since=int(since_ts * 1000 * 1000 * 1000))  # seconds to ns
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

        for row in js["result"][self.symbol]:
            price = float(row[0])
            amount = float(row[1])
            ts = float(row[2])
            trades.append((ts, price, amount))

        return trades, float(js["result"]["last"]) / 1000 / 1000 / 1000  # ns to seconds


watchers = [
    WatcherKraken("btc_eur", "XXBTZEUR"),  
    WatcherKraken("btc_usd", "XXBTZUSD"),  
    WatcherKraken("eth_btc", "XETHXXBT"), 
    WatcherKraken("eth_usd", "XETHZUSD"), 
    WatcherKraken("eth_eur", "XETHZEUR"), 
    WatcherKraken("etc_eur", "XETCZEUR"),  
    WatcherKraken("etc_usd", "XETCZUSD"),  
    WatcherKraken("etc_eth", "XETCXETH"),  
]
