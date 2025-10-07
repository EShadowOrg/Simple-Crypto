import asyncio
import json
import threading
import queue
import time
from typing import Literal
import os
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import pandas as pd
import zipfile
import requests
import websocket as ws
from colorama import Fore
import thread_safe_types as tst

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
            pass

        def __repr__(self):
            return f"<{self.__class__.__name__} for {self.symbol}>"

    def __init__(self, msg_retention=300, order_retention=300, us=True):
        self.stocks = tst.ThreadSafeStockList()
        self.socket = None
        self.actions = queue.Queue()
        self.actions_completed = tst.ThreadSafeCounter()
        self.id = tst.ThreadSafeCounter()
        self.order_list = tst.ThreadSafeOrderList()
        self.msgs = tst.ThreadSafeMsgList()
        self.us = us
        self.us_lock = threading.Lock()
        self.msg_retention = msg_retention
        self.order_retention = order_retention

    def subscribe(self, symbol, instance, currency="USD", event="ticker"):
        if not isinstance(instance, BaseTracker):
            raise ValueError("Instance must be a subclass of BaseTracker")
        if not isinstance(currency, str):
            raise ValueError("Currency must be a string")
        if not isinstance(symbol, str):
            raise ValueError("Symbol must be a string")
        symbols = self.request("/api/v3/exchangeInfo")['symbols']
        if not any(s['symbol'] == f"{symbol.upper()}{currency.upper()}" for s in symbols):
            raise ValueError(f"Symbol {symbol.upper()}{currency.upper()} not found")
        if event not in ["ticker", "trade", "kline_1m", "kline_3m", "kline_5m", "kline_15m", "kline_30m", "kline_1h", "kline_2h", "kline_4h", "kline_6h", "kline_8h", "kline_12h", "kline_1d"]:
            # TODO: list all valid events
            pass
        # TODO: validate event

        currency = currency.upper()
        symbol = symbol.upper()
        if not self.stocks.add(symbol, currency, event, instance):
            if self.socket is not None:
                subid = self.id.count()
                current_event = self.order_list.add(subid)
                self.actions.put({"type": "subscribe", "symbol": symbol, "currency": currency, "event": event, "id": subid})
                current_event.wait(timeout=self.order_retention)
                if current_event.wait(timeout=self.order_retention):
                    return self.msgs.get(subid)
                return -1
            else:
                return 1
        return 0

    def unsubscribe(self, symbol, instance, currency="USD", event="ticker"):
        if not isinstance(instance, BaseTracker):
            raise ValueError("Instance must be a subclass of BaseTracker")
        if not isinstance(currency, str):
            raise ValueError("Currency must be a string")
        if not isinstance(symbol, str):
            raise ValueError("Symbol must be a string")
        currency = currency.upper()
        symbol = symbol.upper()
        if not self.stocks.remove(symbol, currency, event, instance):
            if self.socket is not None:
                subid = self.id.count()
                current_event = self.order_list.add(subid)
                self.actions.put({"type": "unsubscribe", "symbol": symbol, "currency": currency, "event": event, "id": subid})
                if current_event.wait(timeout=self.order_retention):
                    return self.msgs.get(subid)
                return -1
            else:
                return 1
        return 0

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

    def handle_actions(self, max_per_limit=3):
        while not self.actions.empty() and self.socket is not None:
            action = self.actions.get()
            while self.actions_completed.get() > max_per_limit:
                time.sleep(0.05)
            if action["type"] == "subscribe":
                print()
                if self.socket is not None:
                    msg = {
                        "method": "SUBSCRIBE",
                        "params": [f"{action['symbol'].lower()}{action['currency'].lower()}@{action['event'].lower()}"],
                        "id": 1
                    }
                    self.socket.send(json.dumps(msg))
                    self.actions_completed.count()
            elif action["type"] == "unsubscribe":
                if self.socket is not None:
                    msg = {
                        "method": "UNSUBSCRIBE",
                        "params": [f"{action['symbol'].lower()}{action['currency'].lower()}@{action['event'].lower()}"],
                        "id": 1
                    }
                    self.socket.send(json.dumps(msg))
                    self.actions_completed.count()

    async def rate_limit(self, limit=1):
        while self.socket is not None:
            time.sleep(limit)
            self.actions_completed.zero()

    def timeout_cleanup(self):
        while self.socket is not None:
            time.sleep(self.msg_retention)
            self.msgs.cleanup(self.msg_retention)
            self.order_list.cleanup(self.order_retention)

    def on_message(self, msg):
        msg = json.loads(msg)
        self.msgs.add(msg.get('id', None), msg)
        if "result" in msg and msg['result'] is None and 'id' in msg:
            order = self.order_list.get(msg['id'])
            if order is not None:
                order.recieved()
        elif 'e' in msg and 's' in msg:
            if msg['e'] == 'aggTrade':
                event = "aggTrade"
            elif msg['e'] == 'trade':
                event = "trade"
            elif msg['e'] == 'kline':
                event = f"kline_{msg['k']['i']}"
            elif msg['e'] == '1hTicker':
                event = "ticker_1h"
            elif msg['e'] == '4hTicker':
                event = "ticker_4h"
            elif msg['e'] == '24hTicker':
                event = "ticker"
            elif msg['e'] == '1dTicker':
                event = "ticker_1d"

    @staticmethod
    def get_history(symbol: str, currency:str="USD", interval:Literal["1s", "1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "6h", "8h", "12h", "1d"]="1m", num_months:int=1):
        if not interval in ["1s", "1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "6h", "8h", "12h", "1d"]:
            raise ValueError("Invalid interval. Must be one of: '1s', '1m', '3m', '5m', '15m', '30m', '1h', '2h', '4h', '6h', '8h', '12h', '1d'")
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
                    with open(f"Data\\{symbol}\\{symbol}{currency}-{interval}-{month.strftime('%Y-%m')}.zip", "wb") as f:
                        f.write(r.content)
                    with zipfile.ZipFile(f"Data\\{symbol}\\{symbol}{currency}-{interval}-{month.strftime('%Y-%m')}.zip", 'r') as zip_ref:
                        zip_ref.extractall(f"Data\\{symbol}")
                    os.remove(f"Data\\{symbol}\\{symbol}{currency}-{interval}-{month.strftime('%Y-%m')}.zip")
                else:
                    url = url = f"https://data.binance.us/public_data/spot/monthly/klines/{symbol}{currency}/{interval}/{symbol}{currency}-{interval}-{month.strftime('%Y-%m')}.zip"
                    r = requests.get(url)
                    if r.status_code == 200:
                        with open(f"Data\\{symbol}\\{symbol}{currency}-{interval}-{month.strftime('%Y-%m')}.zip", "wb") as f:
                            f.write(r.content)
                        with zipfile.ZipFile(f"Data\\{symbol}\\{symbol}{currency}-{interval}-{month.strftime('%Y-%m')}.zip", 'r') as zip_ref:
                            zip_ref.extractall(f"Data\\{symbol}")
                        os.remove(f"Data\\{symbol}\\{symbol}{currency}-{interval}-{month.strftime('%Y-%m')}.zip")
                    else:
                        pass
            if os.path.isfile(f"Data\\{symbol}\\{symbol}{currency}-{interval}-{month.strftime('%Y-%m')}.csv"):
                months_available.append(month)
        return months_available

    def start(self):
        if self.socket is not None:
            raise RuntimeError("Socket already started")

if __name__ == "__main__":
    print([mo.strftime("%Y-%m") for mo in MarketAccess.get_history("BTC", "USD", "1m", 2)])