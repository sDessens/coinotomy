from queue import PriorityQueue
import time
from threading import Thread, Lock, Event


class TicketSystem(object):
    def __init__(self, seconds_between_requests):
        self.seconds_between_requests = seconds_between_requests
        self.queue = PriorityQueue()

        self.lock = Lock()

    def get_ticket(self, priority):
        with self.lock:
            if self.queue.empty():
                self.start_thread()
            event = Event()
            self.queue.put((priority + time.time(), event))
        event.wait()

    def start_thread(self):
        self.thread = Thread(target=self.run, args=())
        self.thread.start()

    def run(self):
        while True:
            time.sleep(self.seconds_between_requests)
            with self.lock:
                (timeout, event) = self.queue.get(True)

                if timeout <= time.time():
                    event.set()
                else:
                    self.queue.put((timeout, event))
                if self.queue.empty():
                    return


