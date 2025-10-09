import queue
import threading
import time
from typing import Literal
import os
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import zipfile
import requests
import websockets as ws
from colorama import Fore
import thread_safe_types as tst
import asyncio
import json

WS_EVENTS = [
    "aggTrade",
    "trade",
    "kline_1m",
    "kline_3m",
    "kline_5m",
    "kline_15m",
    "kline_30m",
    "kline_1h",
    "kline_2h",
    "kline_4h",
    "kline_6h",
    "kline_8h",
    "kline_12h",
    "kline_1d",
    "kline_3d",
    "kline_1w",
    "kline_1M",
    "ticker_1h"
    "ticker_4h",
    "ticker",
    "miniTicker",
    "bookTicker",
    "depth",
    "depth@100ms",
]

class MarketAccess:
    class BaseTracker:
        def __init__(self, symbol: str, access: 'MarketAccess'):
            if not isinstance(symbol, str):
                raise ValueError("symbol must be a string")
            if not isinstance(access, MarketAccess):
                raise ValueError("access must be an instance of MarketAccess")
            self.symbol = symbol
            self.access = access

        def on_event(self, event, msg):
            print(f"{Fore.CYAN}[EVENT]{Fore.RESET} {self.symbol} received event {event}: {msg}")

        def __repr__(self):
            return f"<{self.__class__.__name__} for {self.symbol}>"

    def __init__(self, msg_retention=300, order_retention=300, max_actions_per_limit=3, rate_limit_seconds=1, us=True):
        self.stocks = tst.ThreadSafeStockList()
        self.connected = False
        self.connected_lock = threading.Lock()
        self.msgs = asyncio.Queue()
        self.us = us
        self.us_lock = threading.Lock()
        self.msg_retention = msg_retention
        self.max_actions_per_limit = max_actions_per_limit
        self.rate_limit_seconds = rate_limit_seconds
        self.thread = None

    def subscribe(self, symbol, instance, currency="USD", event="ticker"):
        if not isinstance(instance, MarketAccess.BaseTracker):
            raise ValueError("Instance must be a subclass of BaseTracker")
        if not isinstance(currency, str):
            raise ValueError("Currency must be a string")
        if not isinstance(symbol, str):
            raise ValueError("Symbol must be a string")
        symbols = self.request("/api/v3/exchangeInfo")['symbols']
        if not any(s['symbol'] == f"{symbol.upper()}{currency.upper()}" for s in symbols):
            raise ValueError(f"Symbol {symbol.upper()}{currency.upper()} not found")
        if event not in WS_EVENTS:
            raise ValueError(f"Event {event} not supported. Supported events: {', '.join(WS_EVENTS)}")

        currency = currency.lower()
        symbol = symbol.lower()
        if not self.stocks.add(f"{symbol}{currency}@{event}", instance):
            return True
        return False

    def unsubscribe(self, symbol, instance, currency="USD", event="ticker"):
        if not isinstance(instance, MarketAccess.BaseTracker):
            raise ValueError("Instance must be a subclass of BaseTracker")
        if not isinstance(currency, str):
            raise ValueError("Currency must be a string")
        if not isinstance(symbol, str):
            raise ValueError("Symbol must be a string")
        if not isinstance(event, str):
            raise ValueError("Event must be a string")

        currency = currency.lower()
        symbol = symbol.lower()
        if not self.stocks.remove(f"{symbol}{currency}@{event}", instance):
            return True
        return False

    @staticmethod
    def static_request(endpoint, us=True):
        if us:
            base_url = "https://api.binance.us"
        else:
            base_url = "https://eapi.binance.com"
        r = requests.get(f"{base_url}{endpoint}")
        if r.status_code == 200:
            return r.json()
        else:
            if r.status_code == 451 and r.json().contains("code") and r.json()['code'] == 0:
                us = not us
                if us:
                    base_url = "https://api.binance.us"
                else:
                    base_url = "https://eapi.binance.com"
                r = requests.get(f"{base_url}{endpoint}")
                if r.status_code == 200:
                    return r.json()
                else:
                    raise ConnectionError(f"Error {r.status_code}: {r.text}")
            else:
                raise ConnectionError(f"Error {r.status_code}: {r.text}")

    def request(self, endpoint):
        if self.us:
            base_url = "https://api.binance.us"
        else:
            base_url = "https://eapi.binance.com"
        r = requests.get(f"{base_url}{endpoint}")
        if r.status_code == 200:
            return r.json()
        else:
            if r.status_code == 451 and r.json().contains("code") and r.json()['code'] == 0:
                with self.us_lock:
                    self.us = not self.us
                if self.us:
                    base_url = "https://api.binance.us"
                else:
                    base_url = "https://eapi.binance.com"
                r = requests.get(f"{base_url}{endpoint}")
                if r.status_code == 200:
                    return r.json()
                else:
                    raise ConnectionError(f"Error {r.status_code}: {r.text}")
            else:
                raise ConnectionError(f"Error {r.status_code}: {r.text}")

    @staticmethod
    def get_history(symbol: str, currency: str = "USD", interval: Literal["1s", "1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "6h", "8h", "12h", "1d"] = "1m", num_months: int = 1):
        if not interval in ["1s", "1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "6h", "8h", "12h", "1d"]:
            raise ValueError(
                "Invalid interval. Must be one of: '1s', '1m', '3m', '5m', '15m', '30m', '1h', '2h', '4h', '6h', '8h', '12h', '1d'")
        if not isinstance(symbol, str):
            raise ValueError("Symbol must be a string")
        if not isinstance(num_months, int):
            raise ValueError("num_months must be an integer")
        if num_months <= 0:
            raise ValueError("num_months must be greater than 0")
        if not isinstance(currency, str):
            raise ValueError("currency must be a string")

        today = datetime.today()
        currency = currency.upper()
        symbol = symbol.upper()
        if not os.path.isdir(f"Data\\{symbol}"):
            os.mkdir(f"Data\\{symbol}")
        months_available = []
        for i in range(num_months, 0, -1):
            month = (today - relativedelta(months=i))
            if not os.path.isfile(f"Data\\{symbol}\\{symbol}{currency}-{interval}-{month.strftime('%Y-%m')}.csv"):
                url = f"https://data.binance.vision/data/spot/monthly/klines/{symbol}{currency}/{interval}/{symbol}{currency}-{interval}-{month.strftime('%Y-%m')}.zip"
                r = requests.get(url)
                if r.status_code == 200:
                    with open(f"Data\\{symbol}\\{symbol}{currency}-{interval}-{month.strftime('%Y-%m')}.zip",
                              "wb") as f:
                        f.write(r.content)
                    with zipfile.ZipFile(f"Data\\{symbol}\\{symbol}{currency}-{interval}-{month.strftime('%Y-%m')}.zip",
                                         'r') as zip_ref:
                        zip_ref.extractall(f"Data\\{symbol}")
                    os.remove(f"Data\\{symbol}\\{symbol}{currency}-{interval}-{month.strftime('%Y-%m')}.zip")
                else:
                    url = url = f"https://data.binance.us/public_data/spot/monthly/klines/{symbol}{currency}/{interval}/{symbol}{currency}-{interval}-{month.strftime('%Y-%m')}.zip"
                    r = requests.get(url)
                    if r.status_code == 200:
                        with open(f"Data\\{symbol}\\{symbol}{currency}-{interval}-{month.strftime('%Y-%m')}.zip",
                                  "wb") as f:
                            f.write(r.content)
                        with zipfile.ZipFile(
                                f"Data\\{symbol}\\{symbol}{currency}-{interval}-{month.strftime('%Y-%m')}.zip",
                                'r') as zip_ref:
                            zip_ref.extractall(f"Data\\{symbol}")
                        os.remove(f"Data\\{symbol}\\{symbol}{currency}-{interval}-{month.strftime('%Y-%m')}.zip")
                    else:
                        pass
            if os.path.isfile(f"Data\\{symbol}\\{symbol}{currency}-{interval}-{month.strftime('%Y-%m')}.csv"):
                months_available.append(month)
        return months_available

    async def msg_processor(self, stream, msg):
        while not self.msgs.empty() or self.connected:
            msg = await self.msgs.get()
            if msg['event'] == 'end':
                return
            stock = self.stocks.get(msg['stream'])
            if stock is not None:
                stock.notify(msg['stream'], msg['data'])

    # TODO: Completely rework _run() and listen(), add run().
    """
    run() calls asyncio.run(self._run()),
    _run() is async and manages all the listeners.
        Subscribe and unsubscribe talk to _run to add new listeners and remove old ones.
    listener is now an object that takes a stream name and a market access object.
        It has a start method that is async and connects to the stream and listens for messages with a `while not self.stopped: msg = await ws.recv()`.
        It has a stop method that sets a flag to stop the listener.
    Edit on_message() to take a `stream` argument to know which stream the message came from.
        on_message() then searches self.stocks for the right stream and calls notify on it.
    """

    async def _run(self):
        listeners = []
        listener_tasks = []
        while self.connected:
            stock_events = self.stocks.keys()
            listened_events = [l.event for l in listeners]
            new_events = [event for event in stock_events if event not in listened_events]
            old_events = [event for event in listened_events if event not in stock_events]
            for event in new_events:
                listener = MarketAccess.Listener(event, self)
                listeners.append(listener)
                listener_tasks.append(asyncio.create_task(listener.start()))
            to_remove = []
            for i, listener in enumerate(listeners):
                if listener.event in old_events:
                    listener.stop()
                    to_remove.append(listener_tasks[i])
                    listeners.remove(listener)
                    listener_tasks.remove(listener_tasks[i])
            await asyncio.wait(to_remove, timeout=10, return_when=asyncio.ALL_COMPLETED)
            for task in to_remove:
                if not task.done():
                    print(f"{Fore.RED}[ERROR]{Fore.RESET} Listener task failed to gracefully stop, cancelling")
                    task.cancel()
            await asyncio.sleep(1)
        for listener in listeners:
            listener.stop()
        await asyncio.wait(listener_tasks, timeout=10, return_when=asyncio.ALL_COMPLETED)
        for task in listener_tasks:
            if not task.done():
                print(f"{Fore.RED}[ERROR]{Fore.RESET} Listener task failed to gracefully stop, cancelling")
                task.cancel()

    def run(self):
        with self.connected_lock:
            self.connected = True
        asyncio.run(self._run())

    def start(self):
        with self.connected_lock:
            if self.connected:
                print(f"{Fore.RED}[ERROR]{Fore.RESET} MarketAccess is already running, cannot start another instance")
                return

        market_thread = threading.Thread(target=self.run, daemon=True)
        market_thread.start()
        self.thread = market_thread

    def stop(self):
        with self.connected_lock:
            self.connected = False
        self.thread.join(timeout=20)
        if self.thread.is_alive():
            print(f"{Fore.RED}[ERROR]{Fore.RESET} Failed to stop thread")
        else:
            print(f"{Fore.GREEN}[STOPPED]{Fore.RESET} Thread stopped gracefully")

if __name__ == "__main__":
    market = MarketAccess()
    market.start()
    time.sleep(2)
    tracker = MarketAccess.BaseTracker("BTC", market)
    market.subscribe("BTC", tracker, event="trade")
    time.sleep(60)
    market.unsubscribe("BTC", tracker, event="trade")
    time.sleep(5)
    market.stop()