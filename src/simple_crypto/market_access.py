import threading
from typing import Literal
import os
from datetime import datetime
from dateutil.relativedelta import relativedelta
import zipfile
import requests
import websockets as ws
from colorama import Fore
from src.simple_crypto import thread_safe_types as tst
import asyncio
import json
import traceback

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
            self.access.logger.log(content=f"Received event {event}: {msg}", title="[EVENT]", title_color=Fore.CYAN)

        def __repr__(self):
            return f"<{self.__class__.__name__} for {self.symbol}>"

    class BaseListener:
        def __init__(self, event: str, market_access: 'MarketAccess'):
            if not isinstance(event, str):
                raise ValueError("event must be a string")
            if not isinstance(market_access, MarketAccess):
                raise ValueError("market_access must be an instance of MarketAccess")
            self.event = event
            self.market_access = market_access
            self.connection = None
            self.stopped = asyncio.Event()

        async def start(self):
            if self.market_access.us:
                url = f"wss://stream.binance.us:9443/ws/{self.event}"
            else:
                url = f"wss://stream.binance.com:9443/ws/{self.event}"
            while not self.stopped.is_set():
                try:
                    async with ws.connect(url) as connection:
                        self.market_access.logger.log(content=f"Listener task for {self.event} started", title="[LISTENER-START]", title_color=Fore.GREEN)
                        self.connection = connection
                        stop_task = asyncio.create_task(self.stopped.wait())
                        while not self.stopped.is_set():
                            msg_task = asyncio.create_task(connection.recv())
                            done, pending = await asyncio.wait(
                                [msg_task, stop_task],
                                return_when=asyncio.FIRST_COMPLETED,
                            )
                            if self.stopped.is_set():
                                msg_task.cancel()
                                self.market_access.logger.log(content=f"Closing listener for {self.event}...", title="[LISTENER-WARNING]", title_color=Fore.YELLOW)
                                await self.connection.close()
                                self.market_access.logger.log(content=f"Connection for {self.event} closed", title="[LISTENER-CONNECTION-CLOSED]", title_color=Fore.GREEN)
                                break
                            for task in done:
                                if task == msg_task:
                                    message = task.result()
                                    await self.market_access.msgs.put({'stream': self.event, 'data': json.loads(message)})
                except ws.exceptions.ConnectionClosed as e:
                    self.market_access.logger.log(content=f"Listener for {self.event} disconnected (line # {traceback.extract_tb(e.__traceback__)[-1].lineno}): {e}. Reconnecting in 5 seconds...", title="[LISTENER-WARNING]", title_color=Fore.YELLOW)
                    await asyncio.sleep(5)
                except Exception as e:
                    self.market_access.logger.log(content=f"Listener for {self.event} encountered an error (line # {traceback.extract_tb(e.__traceback__)[-1].lineno}): {e}. Reconnecting in 5 seconds...", title="[LISTENER-ERROR]", title_color=Fore.RED)
                    await asyncio.sleep(5)
            self.market_access.logger.log(content=f"Listener for {self.event} stopped", title="[LISTENER-STOPPED]", title_color=Fore.GREEN)

        def stop(self):
            self.stopped.set()

    class BaseLogger:
        def __init__(self, filenames:list[str]|None=None, plain:bool=False, timestamps:bool=True, timestamp_format:str='%Y-%m-%d %H-%M-%S.%f'):
            if filenames is None:
                filenames = []
            if not isinstance(filenames, list) or not all([isinstance(file, str) for file in filenames] + [True]):
                raise ValueError("filenames must be a list of strings")
            if not isinstance(plain, bool):
                raise ValueError("plain must be a bool")
            if not isinstance(timestamps, bool):
                raise ValueError("timestamps must be a bool")
            if not isinstance(timestamp_format, str):
                raise ValueError("timestamp_format must be a str")
            self.filenames = filenames
            print(self.filenames)
            self.plain = plain
            self.timestamps = timestamps
            self.timestamps_format = timestamp_format

        def log(self, content:str, content_color:str|None=None, title:str|None=None, title_color:str|None=None):
            if self.plain:
                content_color = None
                title_color = None
            timestamp_insert = f"[{datetime.now().strftime(self.timestamps_format)}] "
            print(f"{timestamp_insert if self.timestamps else ''}{title_color if title_color is not None else ''}{title if title is not None else ''}{Fore.RESET}{' ' if title is not None else ''}{content_color if content_color is not None else ''}{content}{Fore.RESET}")
            for file in self.filenames:
                if os.path.isfile(file):
                    with open(file, 'a') as f:
                        f.write(f"{timestamp_insert if self.timestamps else ''}{(title + ' ') if title is not None else ''}{content}\n")

    def __init__(self, us=True, listener_class=None, logger=None):
        self.stocks = tst.ThreadSafeStockList()
        self.connected = False
        self.connected_lock = threading.Lock()
        self.msgs = asyncio.Queue()
        self.us = us
        self.us_lock = threading.Lock()
        self.thread = None
        if listener_class is not None:
            self.listener_class = listener_class
        else:
            self.listener_class = MarketAccess.BaseListener
        if logger is not None:
            self.logger = logger
        else:
            self.logger = self.BaseLogger()

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
            if not os.path.isfile(f"Data\\{symbol}\\{symbol}-{currency}-{interval}-{month.strftime('%Y-%m')}.csv"):
                url = f"https://data.binance.vision/data/spot/monthly/klines/{symbol}{currency}/{interval}/{symbol}{currency}-{interval}-{month.strftime('%Y-%m')}.zip"
                r = requests.get(url)
                if r.status_code == 200:
                    with open(f"Data\\{symbol}\\{symbol}-{currency}-{interval}-{month.strftime('%Y-%m')}.zip",
                              "wb") as f:
                        f.write(r.content)
                    with zipfile.ZipFile(f"Data\\{symbol}\\{symbol}-{currency}-{interval}-{month.strftime('%Y-%m')}.zip",
                                         'r') as zip_ref:
                        zip_ref.extractall(f"Data\\{symbol}")
                    os.remove(f"Data\\{symbol}\\{symbol}-{currency}-{interval}-{month.strftime('%Y-%m')}.zip")
                else:
                    url = url = f"https://data.binance.us/public_data/spot/monthly/klines/{symbol}{currency}/{interval}/{symbol}{currency}-{interval}-{month.strftime('%Y-%m')}.zip"
                    r = requests.get(url)
                    if r.status_code == 200:
                        with open(f"Data\\{symbol}\\{symbol}-{currency}-{interval}-{month.strftime('%Y-%m')}.zip",
                                  "wb") as f:
                            f.write(r.content)
                        with zipfile.ZipFile(
                                f"Data\\{symbol}\\{symbol}-{currency}-{interval}-{month.strftime('%Y-%m')}.zip",
                                'r') as zip_ref:
                            file = zip_ref.namelist()[0]
                            zip_ref.extractall(f"Data\\{symbol}")
                            os.rename(f"Data\\{symbol}\\{file}", f"Data\\{symbol}\\{symbol}-{currency}-{interval}-{month.strftime('%Y-%m')}.csv")
                        os.remove(f"Data\\{symbol}\\{symbol}-{currency}-{interval}-{month.strftime('%Y-%m')}.zip")
                    else:
                        pass
            if os.path.isfile(f"Data\\{symbol}\\{symbol}-{currency}-{interval}-{month.strftime('%Y-%m')}.csv"):
                months_available.append(month)
        return months_available

    async def msg_processor(self):
        self.logger.log(content=f"Message processor started", title="[MARKET-PROCESSOR-STARTED]", title_color=Fore.GREEN)
        while not self.msgs.empty() or self.connected:
            msg = await self.msgs.get()
            if msg['stream'] == 'end':
                return
            stock = self.stocks.get(msg['stream'])
            if stock is not None:
                stock.notify(msg['stream'], msg['data'])

    async def _run(self):
        listeners = []
        listener_tasks = []
        self.logger.log(content=f"MarketAccess started", title="[MARKET-STARTED]", title_color=Fore.GREEN)
        self.logger.log(content=f"Testing connection...", title="[MARKET-CONNECTION-TEST]", title_color=Fore.YELLOW)
        self.request("/api/v3/ping")
        self.logger.log(content=f"Connection test successful", title="[MARKET-CONNECTION-TEST]", title_color=Fore.GREEN)
        self.logger.log(content=f"Starting message processor...", title="[MARKET-PROCESSOR-START]", title_color=Fore.YELLOW)
        processor_task = asyncio.create_task(self.msg_processor())
        while self.connected:
            stock_events = self.stocks.keys()
            listened_events = [l.event for l in listeners]
            new_events = [event for event in stock_events if event not in listened_events]
            old_events = [event for event in listened_events if event not in stock_events]
            for event in new_events:
                self.logger.log(content=f"Starting listener for {event}...", title="[MARKET-LISTENER-START]", title_color=Fore.YELLOW)
                listener = self.listener_class(event, self)
                listeners.append(listener)
                listener_tasks.append(asyncio.create_task(listener.start()))
            to_remove = []
            stopped_listeners = []
            for i, listener in enumerate(listeners):
                if listener.event in old_events:
                    self.logger.log(content=f"Stopping listener for {listener.event}...", title="[MARKET-LISTENER-STOP]", title_color=Fore.YELLOW)
                    listener.stop()
                    to_remove.append(listener_tasks[i])
                    stopped_listeners.append(listener)
                    listeners.remove(listener)
                    listener_tasks.remove(listener_tasks[i])
            if to_remove:
                await asyncio.wait(to_remove, timeout=20, return_when=asyncio.ALL_COMPLETED)
                for i, task in enumerate(to_remove):
                    if not task.done():
                        self.logger.log(content=f"Listener for {stopped_listeners[i].event} task failed to stop gracefully, cancelling...", title="[MARKET-LISTENER-ERROR]", title_color=Fore.RED)
                        task.cancel()
            await asyncio.sleep(0.1)
        for listener in listeners:
            listener.stop()
        if not all([task.done() for task in listener_tasks]):
            await asyncio.wait(listener_tasks, timeout=20, return_when=asyncio.ALL_COMPLETED)
        for task in listener_tasks:
            if not task.done():
                self.logger.log(content=f"Listener task failed to gracefully stop, cancelling", title="[MARKET-LISTENER-ERROR]", title_color=Fore.RED)
                task.cancel()
        await self.msgs.put({'stream': 'end', 'data': None})
        if not processor_task.done():
            await asyncio.wait([processor_task], timeout=30, return_when=asyncio.ALL_COMPLETED)
        if not processor_task.done():
            self.logger.log(content=f"Message processor failed to stop gracefully, cancelling...", title="[MARKET-PROCESSOR-ERROR]", title_color=Fore.RED)
            processor_task.cancel()

    def run(self):
        with self.connected_lock:
            self.connected = True
        asyncio.run(self._run())

    def start(self):
        with self.connected_lock:
            if self.connected:
                self.logger.log(content=f"MarketAccess is already running, cannot start another instance", title="[MARKET-START-ERROR]", title_color=Fore.RED)
                return

        market_thread = threading.Thread(target=self.run, daemon=True)
        market_thread.start()
        self.thread = market_thread

    def stop(self):
        with self.connected_lock:
            self.connected = False
        self.thread.join(timeout=20)
        if self.thread.is_alive():
            self.logger.log(content="Failed to stop thread", title="[MARKET-STOP-ERROR]", title_color=Fore.RED)
        else:
            self.logger.log(content="Thread stopped gracefully", title="[MARKET-STOPPED]", title_color=Fore.GREEN)