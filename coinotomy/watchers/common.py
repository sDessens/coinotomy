import logging
import time


class Watcher(object):
    def __init__(self, name, interval):
        self.name = name
        self.log = logging.getLogger(name)
        self.interval = interval

    def __del__(self):
        self.unload()

    def name(self):
        return self.name

    def run(self, backend):
        self.log.info("starting")
        self.setup(backend)

        first = True
        while True:
            try:
                self.wait(first)
                self.tick()
                backend.flush()
            except (KeyboardInterrupt, InterruptedError):
                backend.flush()
            except:
                self.log.exception("Exception while processing tick")
            first = False

        self.log.info("gracefully shutting down")
        self.unload()

    def setup(self, backend):
        raise NotImplementedError()

    def tick(self):
        raise NotImplementedError()

    def unload(self):
        raise NotImplementedError()

    def wait(self, first):
        if not first:
            time.sleep(self.interval)
