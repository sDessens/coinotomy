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
            # the tiebreaker makes sure the __lt__ operator on event is never
            # called by PriorityQueue, since it doesn't have one.
            tiebreaker = id(event)
            self.queue.put((priority + time.time(), tiebreaker, event))
        event.wait()

    def start_thread(self):
        self.thread = Thread(target=self.run, args=())
        self.thread.start()

    def run(self):
        while True:
            time.sleep(self.seconds_between_requests)
            with self.lock:
                (timeout, tiebreaker, event) = self.queue.get(True)

                if timeout <= time.time():
                    event.set()
                else:
                    self.queue.put((timeout, tiebreaker, event))
                if self.queue.empty():
                    return


