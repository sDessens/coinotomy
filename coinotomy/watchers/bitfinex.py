import urllib.request
import json

from coinotomy.watchers.common import Watcher
from coinotomy.utils.ticketsystem import TicketSystem

NORMAL_TIMEOUT = 1000
FAST_TIMEOUT = 10

TRADES_TO_QUERY = 1000
MIN_TRADES_FOR_FAST_TIMEOUT = 950

class WatcherBitfinex(Watcher):
    ticket_system = TicketSystem(5)

    def __init__(self, name: str, symbol:str):
        Watcher.__init__(self, "bitfinex." + name, FAST_TIMEOUT)

        self.backend = None
        self.api = BitfinexAPI(symbol, self.log)

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
            self.newest_timestamp = last_trade[0] + 0.001
            self.newest_tid = 0

    def tick(self):
        trades, self.newest_timestamp, self.newest_tid = self.api.more(self.newest_timestamp, self.newest_tid)

        if len(trades) > MIN_TRADES_FOR_FAST_TIMEOUT:
            self.interval = FAST_TIMEOUT
        else:
            self.interval = NORMAL_TIMEOUT

        for ts, p, v in trades:
            self.backend.append(ts, p, v)

    def unload(self):
        if self.backend:
            self.backend.unload()
        self.backend = None

    def wait(self, first):
        self.ticket_system.get_ticket(self.interval)

class BitfinexAPI(object):
    def __init__(self, symbol, log):
        self.symbol = symbol
        self.log = log

    def more(self, since_timestamp=0, since_tid=0):
        return self._parse_response(self._query(since_timestamp), since_timestamp, since_tid)

    def _query(self, since_timestamp):
        url = "https://api.bitfinex.com/v2/trades/%s/hist?limit=%s&start=%s&sort=1" % \
              (self.symbol, TRADES_TO_QUERY, int(since_timestamp * 1000))
        with urllib.request.urlopen(url, timeout=30) as response:
            return str(response.read(), 'ascii')

    def _parse_response(self, html, since_timestamp=0, since_tid=0):
        """
        return (array_of_trades, newest_tid)
        """
        js = json.loads(html)
        trades = []
        newest_timestamp = since_timestamp
        newest_tid = since_tid
        for row in js:
            tid, ts, vol, price = row

            if tid <= since_tid:
                continue

            newest_tid = max(newest_tid, tid)
            newest_timestamp = max(newest_timestamp, ts / 1000.0)
            trades.append((ts / 1000.0, float(price), float(vol)))

        # trades not guaranteed to have ordered timestamps, so sort them.
        return sorted(trades, key=lambda x: x[0]), newest_timestamp, newest_tid


watchers = [
    WatcherBitfinex("btc_usd", "tBTCUSD"),
    WatcherBitfinex("ltc_usd", "tLTCUSD"),
    WatcherBitfinex("ltc_btc", "tLTCBTC"),
    WatcherBitfinex("eth_usd", "tETHUSD"),
    WatcherBitfinex("eth_btc", "tETHBTC"),
    WatcherBitfinex("etc_btc", "tETCBTC"),
    WatcherBitfinex("etc_usd", "tETCUSD"),
    WatcherBitfinex("rrt_usd", "tRRTUSD"),
    WatcherBitfinex("rrt_btc", "tRRTBTC"),
    WatcherBitfinex("zec_usd", "tZECUSD"),
    WatcherBitfinex("zec_btc", "tZECBTC"),
    WatcherBitfinex("xmr_usd", "tXMRUSD"),
    WatcherBitfinex("xmr_btc", "tXMRBTC"),
    WatcherBitfinex("dsh_usd", "tDSHUSD"),
    WatcherBitfinex("dsh_btc", "tDSHBTC"),
    WatcherBitfinex("bcc_btc", "tBCCBTC"),
    WatcherBitfinex("bcu_btc", "tBCUBTC"),
    WatcherBitfinex("bcc_usd", "tBCCUSD"),
    WatcherBitfinex("bcu_usd", "tBCUUSD"),
    WatcherBitfinex("xrp_usd", "tXRPUSD"),
    WatcherBitfinex("xrp_btc", "tXRPBTC"),
    WatcherBitfinex("iot_usd", "tIOTUSD"),
    WatcherBitfinex("iot_btc", "tIOTBTC"),
    WatcherBitfinex("iot_eth", "tIOTETH"),
    WatcherBitfinex("eos_usd", "tEOSUSD"),
    WatcherBitfinex("eos_btc", "tEOSBTC"),
    WatcherBitfinex("eos_eth", "tEOSETH"),
    WatcherBitfinex("san_usd", "tSANUSD"),
    WatcherBitfinex("san_btc", "tSANBTC"),
    WatcherBitfinex("san_eth", "tSANETH"),
    WatcherBitfinex("omg_usd", "tOMGUSD"),
    WatcherBitfinex("omg_btc", "tOMGBTC"),
    WatcherBitfinex("omg_eth", "tOMGETH"),
    WatcherBitfinex("bch_usd", "tBCHUSD"),
    WatcherBitfinex("bch_btc", "tBCHBTC"),
    WatcherBitfinex("bch_eth", "tBCHETH"),
    WatcherBitfinex("neo_usd", "tNEOUSD"),
    WatcherBitfinex("neo_btc", "tNEOBTC"),
    WatcherBitfinex("neo_eth", "tNEOETH"),
    WatcherBitfinex("etp_usd", "tETPUSD"),
    WatcherBitfinex("etp_btc", "tETPBTC"),
    WatcherBitfinex("etp_eth", "tETPETH"),
    WatcherBitfinex("qtm_usd", "tQTMUSD"),
    WatcherBitfinex("qtm_btc", "tQTMBTC"),
    WatcherBitfinex("qtm_eth", "tQTMETH"),
    WatcherBitfinex("bt1_usd", "tBT1USD"),
    WatcherBitfinex("bt2_usd", "tBT2USD"),
    WatcherBitfinex("bt1_btc", "tBT1BTC"),
    WatcherBitfinex("bt2_btc", "tBT2BTC"),
    WatcherBitfinex("avt_usd", "tAVTUSD"),
    WatcherBitfinex("avt_btc", "tAVTBTC"),
    WatcherBitfinex("avt_eth", "tAVTETH"),
    WatcherBitfinex("edo_usd", "tEDOUSD"),
    WatcherBitfinex("edo_btc", "tEDOBTC"),
    WatcherBitfinex("edo_eth", "tEDOETH"),
    WatcherBitfinex("btg_usd", "tBTGUSD"),
    WatcherBitfinex("btg_btc", "tBTGBTC"),
    WatcherBitfinex("dat_usd", "tDATUSD"),
    WatcherBitfinex("dat_btc", "tDATBTC"),
    WatcherBitfinex("dat_eth", "tDATETH"),
]
