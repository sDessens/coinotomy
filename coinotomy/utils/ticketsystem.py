from queue import Queue
import time
from threading import Thread


class TicketSystem(object):
    def __init__(self, seconds_between_requests):
        self.seconds_between_requests = seconds_between_requests
        self.queue = Queue(maxsize=1)
        self.thread = Thread(target=self.run, args=())
        self.thread.start()

    def get_ticket(self):
        # Blocks forever if queue is not empty
        # under both windows 10 and debian python3.4, threads are unblocked
        # in the same order as they are blocked
        self.queue.put(None)

    def run(self):
        while True:
            time.sleep(self.seconds_between_requests)
            # Remove an item from the queue every so-much seconds
            self.queue.get(True)
