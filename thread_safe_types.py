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

class ThreadSafeOrder:
    def __init__(self, id: int, event: threading.Event):
        if isinstance(id, int):
            raise ValueError("id must be an integer")
        if not isinstance(event, threading.Event):
            raise ValueError("event must be an instance of threading.Event")
        self.event = event
        self.id = id
        self.time = datetime.now()

    def received(self):
        self.event.set()

    def late(self, seconds: int):
        return (datetime.now() - self.time).total_seconds() > seconds

class ThreadSafeOrderList:
    def __init__(self):
        self.orders = {}
        self.lock = threading.Lock()

    def add(self, id: int):
        if not isinstance(id, int):
            raise ValueError("id must be an integer")
        event = threading.Event()
        with self.lock:
            self.orders[id] = ThreadSafeOrder(id, event)
        return event

    def get(self, id: int):
        if not isinstance(id, int):
            raise ValueError("id must be an integer")
        with self.lock:
            return self.orders.get(id, None)

    def remove(self, id: int):
        if not isinstance(id, int):
            raise ValueError("id must be an integer")
        with self.lock:
            if id in self.orders:
                del self.orders[id]

    def __contains__(self, id: int):
        if not isinstance(id, int):
            raise ValueError("id must be an integer")
        with self.lock:
            return id in self.orders

    def cleanup(self, timeout: int):
        if not isinstance(timeout, int):
            raise ValueError("timeout must be an integer")
        with self.lock:
            to_remove = [id for id, order in self.orders.items() if order.late(timeout)]
            for id in to_remove:
                del self.orders[id]

class ThreadSafeStock:
    def __init__(self, symbol: str, currency: str, event: str, trackers: list):
        if not isinstance(symbol, str):
            raise ValueError("symbol must be a string")
        if not isinstance(currency, str):
            raise ValueError("currency must be a string")
        if not isinstance(event, str):
            raise ValueError("event must be a string")
        if not isinstance(trackers, list):
            raise ValueError("trackers must be a list")
        self.event = f"{symbol}{currency}@{event}"
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

    def add(self, symbol: str, currency: str, event: str, instance):
        if not isinstance(symbol, str):
            raise ValueError("symbol must be a string")
        if not isinstance(currency, str):
            raise ValueError("currency must be a string")
        if not isinstance(event, str):
            raise ValueError("event must be a string")
        currency = currency.upper()
        symbol = symbol.upper()
        key = f"{symbol}{currency}@{event}"
        with self.lock:
            if key not in self.stocks:
                self.stocks[key] = ThreadSafeStock(symbol, currency, event, [instance])
                return False
            else:
                self.stocks[key].add_tracker(instance)
                return True

    def remove(self, symbol: str, currency: str, event: str, instance):
        if not isinstance(symbol, str):
            raise ValueError("symbol must be a string")
        if not isinstance(currency, str):
            raise ValueError("currency must be a string")
        if not isinstance(event, str):
            raise ValueError("event must be a string")
        currency = currency.upper()
        symbol = symbol.upper()
        key = f"{symbol}{currency}@{event}"
        with self.lock:
            if key in self.stocks:
                if self.stocks[key].remove_tracker(instance):
                    if not self.stocks[key].has_trackers():
                        del self.stocks[key]
                    return True
                return False
            return False

    def get(self, symbol: str, currency: str, event: str):
        if not isinstance(symbol, str):
            raise ValueError("symbol must be a string")
        if not isinstance(currency, str):
            raise ValueError("currency must be a string")
        if not isinstance(event, str):
            raise ValueError("event must be a string")
        currency = currency.upper()
        symbol = symbol.upper()
        key = f"{symbol}{currency}@{event}"
        with self.lock:
            return self.stocks.get(key, None)

    def getbykey(self, key: str):
        if not isinstance(key, str):
            raise ValueError("key must be a string")
        with self.lock:
            return self.stocks.get(key, None)

    def __getitem__(self, symbol, currency, event):
        return self.get(symbol, currency, event)

    def __contains__(self, key):
        with self.lock:
            return key in self.stocks

class Msg:
    def __init__(self, content:dict, id:int|None=None):
        self.id = id
        self.json = content
        self.time = datetime.now()

    def late(self, seconds:int):
        return (datetime.now() - self.time).total_seconds() > seconds

    def __eq__(self, other:int):
        if not isinstance(other, int):
            raise ValueError("Can only compare Msg with an integer id")
        if self.id is not None:
            return self.id == other
        return False

class ThreadSafeMsgList:
    def __init__(self):
        self.msgs = []
        self.lock = threading.Lock()

    def add(self, content:dict, id:int|None=None):
        if id is not None and not isinstance(id, int):
            raise ValueError("id must be an integer or None")
        if not isinstance(content, dict):
            raise ValueError("content must be a dictionary")
        with self.lock:
            msg = Msg(content, id)
            self.msgs.append(msg)
            return msg

    def get(self, id:int):
        if not isinstance(id, int):
            raise ValueError("id must be an integer")
        with self.lock:
            for msg in self.msgs:
                if msg == id:
                    return msg
            return None

    def remove(self, id:int):
        if not isinstance(id, int):
            raise ValueError("id must be an integer")
        with self.lock:
            for i, msg in enumerate(self.msgs):
                if msg == id:
                    return self.msgs.pop(i)
            return None

    def cleanup(self, timeout:int):
        if not isinstance(timeout, int):
            raise ValueError("timeout must be an integer")
        with self.lock:
            self.msgs = [msg for msg in self.msgs if not msg.late(timeout)]