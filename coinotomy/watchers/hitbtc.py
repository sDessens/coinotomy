import json
import urllib.request

from coinotomy.utils.ticketsystem import TicketSystem
from coinotomy.watchers.common import Watcher

NORMAL_TIMEOUT = 1
SLOW_TIMEOUT = 10*60


class WatcherHitbtc(Watcher):
    # share rate limiting between all hitbtc instances
    ticket_system = TicketSystem(NORMAL_TIMEOUT)

    def __init__(self, name, symbol):
        Watcher.__init__(self, "hitbtc." + name, 1)

        self.backend = None
        self.api = HitbtcApi(symbol, self.log)

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
        if last_trade is None:
            self.newest_timestamp = 0
            self.newest_tid = 0
        else:
            self.newest_timestamp = last_trade[0] + 0.0001
            self.newest_tid = 0

    def tick(self):
        self.interval = SLOW_TIMEOUT
        if self.newest_tid:
            # trades filtered by api
            trades, self.newest_tid = self.api.more_tid(self.newest_tid)
        else:
            trades, self.newest_tid = self.api.more_ts(self.newest_timestamp)

        for ts, p, v in trades:
            self.backend.append(ts, p, v)
        self.backend.flush()

        if len(trades) == HitbtcApi.MAX_TRADES:
            self.interval = NORMAL_TIMEOUT

    def unload(self):
        if self.backend:
            self.backend.unload()
        self.backend = None

    def wait(self, first):
        self.ticket_system.get_ticket(self.interval)


class HitbtcApi(object):
    MAX_TRADES = 1000

    def __init__(self, symbol, log):
        self.symbol = symbol
        self.log = log

    def more_tid(self, since_tid=0):
        return self._parse_response(self._query(False, since_tid), since_tid=since_tid)

    def more_ts(self, since_ts):
        return self._parse_response(self._query(True, since_ts), since_tid=0)

    def _query(self, ts_or_tid, since):
        if ts_or_tid:
            url = 'https://api.hitbtc.com/api/1/public/%s/trades?by=ts&sort=asc&from=%i' % \
                (self.symbol, since * 1000)
        else:
            url = 'https://api.hitbtc.com/api/1/public/%s/trades?by=trade_id&sort=asc&from=%i' % \
                  (self.symbol, since)
        with urllib.request.urlopen(url, timeout=10) as response:
            return str(response.read(), 'ascii')

    def _parse_response(self, html, since_tid=0):
        """
        return (array_of_trades, newest_tid)
        """
        js = json.loads(html)
        trades = []
        newest_tid = since_tid
        for row in js['trades']:
            tid = int(row[0])
            if tid <= since_tid:
                continue

            newest_tid = max(newest_tid, tid)
            price = float(row[1])
            amount = float(row[2])
            timestamp = row[3] / 1000
            trades.append((timestamp, price, amount))

        # sort just in case
        return sorted(trades, key=lambda x: x[0]), newest_tid


watchers = [
    WatcherHitbtc("bcn_btc", "BCNBTC"),
    WatcherHitbtc("btc_usd", "BTCUSD"),
    WatcherHitbtc("dash_btc", "DASHBTC"),
    WatcherHitbtc("doge_btc", "DOGEBTC"),
    WatcherHitbtc("dsh_btc", "DSHBTC"),
    WatcherHitbtc("emc_btc", "EMCBTC"),
    WatcherHitbtc("eth_btc", "ETHBTC"),
    WatcherHitbtc("fcn_btc", "FCNBTC"),
    WatcherHitbtc("lsk_btc", "LSKBTC"),
    WatcherHitbtc("ltc_btc", "LTCBTC"),
    WatcherHitbtc("ltc_usd", "LTCUSD"),
    WatcherHitbtc("nxt_btc", "NXTBTC"),
    WatcherHitbtc("qcn_btc", "QCNBTC"),
    WatcherHitbtc("sbd_btc", "SBDBTC"),
    WatcherHitbtc("sc_btc", "SCBTC"),
    WatcherHitbtc("steem_btc", "STEEMBTC"),
    WatcherHitbtc("xdn_btc", "XDNBTC"),
    WatcherHitbtc("xem_btc", "XEMBTC"),
    WatcherHitbtc("xmr_btc", "XMRBTC"),
    WatcherHitbtc("ardr_btc", "ARDRBTC"),
    WatcherHitbtc("zec_btc", "ZECBTC"),
    WatcherHitbtc("waves_btc", "WAVESBTC"),
    WatcherHitbtc("bch_btc", "BCHBTC"),
]
