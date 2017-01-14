import json
import logging
import asyncio
import websockets
import time

from coinotomy.watchers.common import Watcher


class WatcherChbtc(Watcher):
    def __init__(self, pair):
        self.pair = pair
        self.name = 'chbtc.' + pair
        self.log = logging.getLogger(self.name + pair)

    def __del__(self):
        self.unload()

    def name(self):
        return self.name

    def run(self, backend):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        while True:
            try:
                loop.run_until_complete(self.connect(backend))
            except Exception:
                logging.error('retry')
                time.sleep(5)

    @asyncio.coroutine
    def connect(self, backend):
        newest_tid = 0
        newest_timestamp = 0
        for ts, price, vol in backend.rlines():
            newest_timestamp = ts
            break

        websocket = yield from websockets.connect('wss://api.chbtc.com:9999/websocket')

        subscription_channel = self.pair + '_trades'

        # subscribe to pair
        yield from websocket.send(str({'event': 'addChannel', 'channel': subscription_channel}))

        while True:
            message = yield from websocket.recv()
            js = json.loads(message)
            channel = js['channel']

            if channel != subscription_channel:
                self.log.warn('unknown channel {}'.format(channel))

            for line in js['data']:
                timestamp = line['date']
                amount = line['amount']
                price = line['price']
                tid = line['tid']
                if newest_tid == 0 and timestamp <= newest_timestamp:
                    continue
                if newest_tid >= tid:
                    continue
                newest_tid = tid
                backend.append(timestamp, price, amount)
            backend.flush()

watchers = [
    WatcherChbtc("btc_cny"),
    WatcherChbtc("eth_cny"),
    WatcherChbtc("etc_cny"),
]

