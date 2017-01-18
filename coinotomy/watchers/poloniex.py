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
        if len(trades) == self.api.window_size:
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
    WatcherPoloniex("etc_usdt", "USDT_ETC"),
    WatcherPoloniex("dsh_btc", "BTC_DASH"),
    WatcherPoloniex("dsh_usdt", "USDT_DASH"),

	WatcherPoloniex('xmr_btc', 'BTC_XMR'),
	WatcherPoloniex('fct_btc', 'BTC_FCT'),
	WatcherPoloniex('sys_btc', 'BTC_SYS'),
	WatcherPoloniex('lsk_btc', 'BTC_LSK'),
	WatcherPoloniex('maid_btc', 'BTC_MAID'),
	WatcherPoloniex('nxt_btc', 'BTC_NXT'),
	WatcherPoloniex('ltc_btc', 'BTC_LTC'),
	WatcherPoloniex('doge_btc', 'BTC_DOGE'),
	WatcherPoloniex('xrp_btc', 'BTC_XRP'),
	WatcherPoloniex('steem_btc', 'BTC_STEEM'),
	WatcherPoloniex('bts_btc', 'BTC_BTS'),
	WatcherPoloniex('amp_btc', 'BTC_AMP'),
	WatcherPoloniex('dgb_btc', 'BTC_DGB'),
	WatcherPoloniex('xem_btc', 'BTC_XEM'),
	WatcherPoloniex('vox_btc', 'BTC_VOX'),
	WatcherPoloniex('exp_btc', 'BTC_EXP'),
	WatcherPoloniex('lbc_btc', 'BTC_LBC'),
	WatcherPoloniex('bcy_btc', 'BTC_BCY'),
	WatcherPoloniex('sc_btc', 'BTC_SC'),
	WatcherPoloniex('sbd_btc', 'BTC_SBD'),
	WatcherPoloniex('xcp_btc', 'BTC_XCP'),
	WatcherPoloniex('naut_btc', 'BTC_NAUT'),
	WatcherPoloniex('note_btc', 'BTC_NOTE'),
	WatcherPoloniex('btcd_btc', 'BTC_BTCD'),
	WatcherPoloniex('dcr_btc', 'BTC_DCR'),
	WatcherPoloniex('game_btc', 'BTC_GAME'),
	WatcherPoloniex('sjcx_btc', 'BTC_SJCX'),
	WatcherPoloniex('omni_btc', 'BTC_OMNI'),
	WatcherPoloniex('pot_btc', 'BTC_POT'),
	WatcherPoloniex('bbr_btc', 'BTC_BBR'),
	WatcherPoloniex('str_btc', 'BTC_STR'),
	WatcherPoloniex('ioc_btc', 'BTC_IOC'),
	WatcherPoloniex('qora_btc', 'BTC_QORA'),
	WatcherPoloniex('xvc_btc', 'BTC_XVC'),
	WatcherPoloniex('vrc_btc', 'BTC_VRC'),
	WatcherPoloniex('rads_btc', 'BTC_RADS'),
	WatcherPoloniex('huc_btc', 'BTC_HUC'),
	WatcherPoloniex('myr_btc', 'BTC_MYR'),
	WatcherPoloniex('burst_btc', 'BTC_BURST'),
	WatcherPoloniex('emc2_btc', 'BTC_EMC2'),
	WatcherPoloniex('clam_btc', 'BTC_CLAM'),
	WatcherPoloniex('bcn_btc', 'BTC_BCN'),
	WatcherPoloniex('nav_btc', 'BTC_NAV'),
	WatcherPoloniex('via_btc', 'BTC_VIA'),
	WatcherPoloniex('grc_btc', 'BTC_GRC'),
	WatcherPoloniex('vtc_btc', 'BTC_VTC'),
	WatcherPoloniex('qbk_btc', 'BTC_QBK'),
	WatcherPoloniex('neos_btc', 'BTC_NEOS'),
	WatcherPoloniex('unity_btc', 'BTC_UNITY'),
	WatcherPoloniex('blk_btc', 'BTC_BLK'),
	WatcherPoloniex('ppc_btc', 'BTC_PPC'),
	WatcherPoloniex('sdc_btc', 'BTC_SDC'),
	WatcherPoloniex('xbc_btc', 'BTC_XBC'),
	WatcherPoloniex('rby_btc', 'BTC_RBY'),
	WatcherPoloniex('btm_btc', 'BTC_BTM'),
	WatcherPoloniex('qtl_btc', 'BTC_QTL'),
	WatcherPoloniex('nmc_btc', 'BTC_NMC'),
	WatcherPoloniex('fldc_btc', 'BTC_FLDC'),
	WatcherPoloniex('xpm_btc', 'BTC_XPM'),
	WatcherPoloniex('nsr_btc', 'BTC_NSR'),
	WatcherPoloniex('pink_btc', 'BTC_PINK'),
	WatcherPoloniex('nobl_btc', 'BTC_NOBL'),
	WatcherPoloniex('flo_btc', 'BTC_FLO'),
	WatcherPoloniex('bits_btc', 'BTC_BITS'),
	WatcherPoloniex('ric_btc', 'BTC_RIC'),
	WatcherPoloniex('bela_btc', 'BTC_BELA'),
	WatcherPoloniex('hz_btc', 'BTC_HZ'),
	WatcherPoloniex('xmg_btc', 'BTC_XMG'),
	WatcherPoloniex('cure_btc', 'BTC_CURE'),
	WatcherPoloniex('c2_btc', 'BTC_C2'),
]
