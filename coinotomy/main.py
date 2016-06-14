import logging
import os

from threading import Thread

from coinotomy.config.config import STORAGE_CLASS, STORAGE_DIRECTORY, WATCHERS


log = logging.getLogger("main")
logging.basicConfig(filename=os.path.join(STORAGE_DIRECTORY, 'log.txt'),
                    filemode='a',
                    datefmt='%H:%M:%S',
                    level=logging.DEBUG)


def launch_worker(watcher):
    backend = STORAGE_CLASS(os.path.join(STORAGE_DIRECTORY, watcher.name))
    watcher.run(backend)


def main():
    threads = []

    log.info("starting main thread")

    # launch all watchers
    for watcher in WATCHERS:
        thread = Thread(target=launch_worker, args=(watcher,))
        thread.start()
        threads.append(thread)

    # block until all threads terminate
    for thread in threads:
        thread.join()

    log.info("terminating main thread")


if __name__ == '__main__':
    main()