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

        sleep = False
        while True:
            try:
                if sleep:
                    time.sleep(self.interval)
                self.tick()
                backend.flush()
            except (KeyboardInterrupt, InterruptedError):
                backend.flush()
            except:
                self.log.exception("Exception while processing tick")
            sleep = True

        self.log.info("gracefully shutting down")
        self.unload()

    def setup(self, backend):
        raise NotImplementedError()

    def tick(self):
        raise NotImplementedError()

    def unload(self):
        raise NotImplementedError()


