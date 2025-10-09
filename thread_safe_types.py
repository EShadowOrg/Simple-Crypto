import threading
from datetime import datetime

class ThreadSafeCounter:
    def __init__(self, initial=0):
        self.value = initial
        self.lock = threading.Lock()

    def count(self, step=1):
        with self.lock:
            self.value += step
            return self.value

    def decount(self, step=1):
        with self.lock:
            self.value -= step
            return self.value

    def zero(self):
        with self.lock:
            self.value = 0
            return self.value

    def get(self):
        with self.lock:
            return self.value

class ThreadSafeStock:
    def __init__(self, event: str, trackers: list):
        if not isinstance(event, str):
            raise ValueError("event must be a string")
        if not isinstance(trackers, list):
            raise ValueError("trackers must be a list")
        self.event = event
        self.trackers = trackers
        self.lock = threading.Lock()

    def add_tracker(self, instance):
        with self.lock:
            if instance not in self.trackers:
                self.trackers.append(instance)
                return True
            return False

    def remove_tracker(self, instance):
        with self.lock:
            if instance in self.trackers:
                self.trackers.remove(instance)
                return True
            return False

    def has_trackers(self):
        with self.lock:
            return len(self.trackers) > 0

    def notify(self, event, data):
        with self.lock:
            for tracker in self.trackers:
                try:
                    tracker.on_event(event, data)
                except Exception as e:
                    print(f"Error notifying tracker: {e}")
                    continue

    def __contains__(self, instance):
        with self.lock:
            return instance in self.trackers

class ThreadSafeStockList:
    def __init__(self):
        self.stocks = {}
        self.lock = threading.Lock()

    def add(self, event: str, instance):
        if not isinstance(event, str):
            raise ValueError("event must be a string")
        with self.lock:
            if event not in self.stocks:
                self.stocks[event] = ThreadSafeStock(event, [instance])
                return False
            else:
                self.stocks[event].add_tracker(instance)
                return True

    def remove(self, event: str, instance):
        if not isinstance(event, str):
            raise ValueError("event must be a string")
        with self.lock:
            if event in self.stocks:
                if self.stocks[event].remove_tracker(instance):
                    if not self.stocks[event].has_trackers():
                        del self.stocks[event]
                    return True
                return False
            return False

    def get(self, event: str):
        if not isinstance(event, str):
            raise ValueError("event must be a string")
        with self.lock:
            return self.stocks.get(event, None)

    def __getitem__(self, event):
        return self.get(event)

    def __contains__(self, key):
        with self.lock:
            return key in self.stocks

    def keys(self):
        with self.lock:
            return list(self.stocks.keys())