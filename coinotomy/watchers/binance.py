import urllib.request
import json
from dateutil import parser

from coinotomy.watchers.common import Watcher
from coinotomy.utils.ticketsystem import TicketSystem
from coinotomy.config import config

NORMAL_TIMEOUT = 1000
FAST_TIMEOUT = 5

# maximum trades received per call call
MAX_TRADES = 500


class WatcherBinance(Watcher):
    ticket_system = TicketSystem(6)

    def __init__(self, name: str, symbol: str):
        Watcher.__init__(self, "binance." + name, NORMAL_TIMEOUT)

        self.backend = None
        self.api = BinanceAPI(symbol, self.log)
        self.interval = FAST_TIMEOUT
        self.newest_tid = 0

    def setup(self, backend):
        self.backend = backend

        # inefficient
        self.newest_tid = 0
        for line in self.backend.lines():
            self.newest_tid += 1

    def tick(self):
        trades, self.newest_tid = self.api.more_since_tid(self.newest_tid)

        for ts, p, v in trades:
            self.backend.append(ts, p, v)

        if len(trades) == MAX_TRADES:
            self.interval = FAST_TIMEOUT
        else:
            self.interval = NORMAL_TIMEOUT

    def unload(self):
        if self.backend:
            self.backend.unload()
        self.backend = None

    def wait(self, first):
        self.ticket_system.get_ticket(self.interval)


class BinanceAPI(object):
    def __init__(self, symbol, log):
        self.symbol = symbol
        self.log = log

    URL = \
        'https://api.binance.com/api/v1/historicalTrades?symbol={symbol}&fromId={fromid}'
    HEADERS = {
        'User-Agent': 'python',
        'X-MBX-APIKEY': config.BINANCE_API_KEY
    }

    def more_since_tid(self, since_id):
        url = self.URL.format(symbol=self.symbol,
                              fromid=since_id if since_id == 0 else since_id + 1)
        html = self._query(url)
        return self._parse_response(html, since_id)

    def parse_date(self, date):
        return parser.parse(date + '+0000').timestamp()

    def _query(self, url):
        request = urllib.request.Request(url, headers=self.HEADERS)
        with urllib.request.urlopen(request, timeout=10) as response:
            return str(response.read(), 'ascii')

    def _parse_response(self, html, since_id):
        """
        return array_of_trades
        """
        js = json.loads(html)
        trades = []

        for row in js:
            id = int(row['id'])
            ts = float(row['time']) / 1000.0
            price = float(row['price'])
            amount = float(row['qty'])
            if id >= since_id:
                trades.append((ts, price, amount))
                since_id = id

        trades = sorted(trades)
        return trades, since_id

watchers = [
    WatcherBinance("eth_btc", "ETHBTC"),
    WatcherBinance("btc_usdt", "BTCUSDT"),
    WatcherBinance("eth_usdt", "ETHUSDT"),
]
