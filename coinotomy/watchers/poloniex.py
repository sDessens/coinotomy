import urllib.request
import json
from dateutil import parser
import time

from coinotomy.watchers.common import Watcher
from coinotomy.utils.ticketsystem import TicketSystem

NORMAL_TIMEOUT = 1000
FAST_TIMEOUT = 50

INIT_WINDOW_SIZE = 60 * 60 * 24 * 30
MAX_TRADES = 50000


class WatcherPoloniex(Watcher):
    ticket_system = TicketSystem(50)

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
    WatcherPoloniex("bcn_btc", "BTC_BCN"),
    WatcherPoloniex("bela_btc", "BTC_BELA"),
    WatcherPoloniex("blk_btc", "BTC_BLK"),
    WatcherPoloniex("btcd_btc", "BTC_BTCD"),
    WatcherPoloniex("btm_btc", "BTC_BTM"),
    WatcherPoloniex("bts_btc", "BTC_BTS"),
    WatcherPoloniex("burst_btc", "BTC_BURST"),
    WatcherPoloniex("clam_btc", "BTC_CLAM"),
    WatcherPoloniex("dash_btc", "BTC_DASH"),
    WatcherPoloniex("dgb_btc", "BTC_DGB"),
    WatcherPoloniex("doge_btc", "BTC_DOGE"),
    WatcherPoloniex("emc2_btc", "BTC_EMC2"),
    WatcherPoloniex("fldc_btc", "BTC_FLDC"),
    WatcherPoloniex("flo_btc", "BTC_FLO"),
    WatcherPoloniex("game_btc", "BTC_GAME"),
    WatcherPoloniex("grc_btc", "BTC_GRC"),
    WatcherPoloniex("huc_btc", "BTC_HUC"),
    WatcherPoloniex("ltc_btc", "BTC_LTC"),
    WatcherPoloniex("maid_btc", "BTC_MAID"),
    WatcherPoloniex("omni_btc", "BTC_OMNI"),
    WatcherPoloniex("naut_btc", "BTC_NAUT"),
    WatcherPoloniex("nav_btc", "BTC_NAV"),
    WatcherPoloniex("neos_btc", "BTC_NEOS"),
    WatcherPoloniex("nmc_btc", "BTC_NMC"),
    WatcherPoloniex("note_btc", "BTC_NOTE"),
    WatcherPoloniex("nxt_btc", "BTC_NXT"),
    WatcherPoloniex("pink_btc", "BTC_PINK"),
    WatcherPoloniex("pot_btc", "BTC_POT"),
    WatcherPoloniex("ppc_btc", "BTC_PPC"),
    WatcherPoloniex("ric_btc", "BTC_RIC"),
    WatcherPoloniex("sjcx_btc", "BTC_SJCX"),
    WatcherPoloniex("str_btc", "BTC_STR"),
    WatcherPoloniex("sys_btc", "BTC_SYS"),
    WatcherPoloniex("via_btc", "BTC_VIA"),
    WatcherPoloniex("xvc_btc", "BTC_XVC"),
    WatcherPoloniex("vrc_btc", "BTC_VRC"),
    WatcherPoloniex("vtc_btc", "BTC_VTC"),
    WatcherPoloniex("xbc_btc", "BTC_XBC"),
    WatcherPoloniex("xcp_btc", "BTC_XCP"),
    WatcherPoloniex("xem_btc", "BTC_XEM"),
    WatcherPoloniex("xmr_btc", "BTC_XMR"),
    WatcherPoloniex("xpm_btc", "BTC_XPM"),
    WatcherPoloniex("xrp_btc", "BTC_XRP"),
    WatcherPoloniex("btc_usdt", "USDT_BTC"),
    WatcherPoloniex("dash_usdt", "USDT_DASH"),
    WatcherPoloniex("ltc_usdt", "USDT_LTC"),
    WatcherPoloniex("nxt_usdt", "USDT_NXT"),
    WatcherPoloniex("str_usdt", "USDT_STR"),
    WatcherPoloniex("xmr_usdt", "USDT_XMR"),
    WatcherPoloniex("xrp_usdt", "USDT_XRP"),
    WatcherPoloniex("bcn_xmr", "XMR_BCN"),
    WatcherPoloniex("blk_xmr", "XMR_BLK"),
    WatcherPoloniex("btcd_xmr", "XMR_BTCD"),
    WatcherPoloniex("dash_xmr", "XMR_DASH"),
    WatcherPoloniex("ltc_xmr", "XMR_LTC"),
    WatcherPoloniex("maid_xmr", "XMR_MAID"),
    WatcherPoloniex("nxt_xmr", "XMR_NXT"),
    WatcherPoloniex("eth_btc", "BTC_ETH"),
    WatcherPoloniex("eth_usdt", "USDT_ETH"),
    WatcherPoloniex("sc_btc", "BTC_SC"),
    WatcherPoloniex("bcy_btc", "BTC_BCY"),
    WatcherPoloniex("exp_btc", "BTC_EXP"),
    WatcherPoloniex("fct_btc", "BTC_FCT"),
    WatcherPoloniex("rads_btc", "BTC_RADS"),
    WatcherPoloniex("amp_btc", "BTC_AMP"),
    WatcherPoloniex("dcr_btc", "BTC_DCR"),
    WatcherPoloniex("lsk_btc", "BTC_LSK"),
    WatcherPoloniex("lsk_eth", "ETH_LSK"),
    WatcherPoloniex("lbc_btc", "BTC_LBC"),
    WatcherPoloniex("steem_btc", "BTC_STEEM"),
    WatcherPoloniex("steem_eth", "ETH_STEEM"),
    WatcherPoloniex("sbd_btc", "BTC_SBD"),
    WatcherPoloniex("etc_btc", "BTC_ETC"),
    WatcherPoloniex("etc_eth", "ETH_ETC"),
    WatcherPoloniex("etc_usdt", "USDT_ETC"),
    WatcherPoloniex("rep_btc", "BTC_REP"),
    WatcherPoloniex("rep_usdt", "USDT_REP"),
    WatcherPoloniex("rep_eth", "ETH_REP"),
    WatcherPoloniex("ardr_btc", "BTC_ARDR"),
    WatcherPoloniex("zec_btc", "BTC_ZEC"),
    WatcherPoloniex("zec_eth", "ETH_ZEC"),
    WatcherPoloniex("zec_usdt", "USDT_ZEC"),
    WatcherPoloniex("zec_xmr", "XMR_ZEC"),
    WatcherPoloniex("strat_btc", "BTC_STRAT"),
    WatcherPoloniex("nxc_btc", "BTC_NXC"),
    WatcherPoloniex("pasc_btc", "BTC_PASC"),
    WatcherPoloniex("gnt_btc", "BTC_GNT"),
    WatcherPoloniex("gnt_eth", "ETH_GNT"),
    WatcherPoloniex("gno_btc", "BTC_GNO"),
    WatcherPoloniex("gno_eth", "ETH_GNO"),
    WatcherPoloniex("bch_btc", "BTC_BCH"),
    WatcherPoloniex("bch_eth", "ETH_BCH"),
    WatcherPoloniex("bch_usdt", "USDT_BCH"),
    WatcherPoloniex("zrx_btc", "BTC_ZRX"),
    WatcherPoloniex("zrx_eth", "ETH_ZRX"),
    WatcherPoloniex("cvc_btc", "BTC_CVC"),
    WatcherPoloniex("cvc_eth", "ETH_CVC"),
    WatcherPoloniex("omg_btc", "BTC_OMG"),
    WatcherPoloniex("omg_eth", "ETH_OMG"),
    WatcherPoloniex("gas_btc", "BTC_GAS"),
    WatcherPoloniex("gas_eth", "ETH_GAS"),
    WatcherPoloniex("storj_btc", "BTC_STORJ"),
]
