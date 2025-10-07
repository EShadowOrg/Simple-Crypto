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

class BaseTracker:
    def __init__(self, symbol, access: 'MarketAccess'):
        self.symbol = symbol
        if not isinstance(access, MarketAccess):
            raise ValueError("access must be an instance of MarketAccess")
        self.access = access

    def on_event(self, event, data):
        pass

class MarketAccess:
    def __init__(self):
        self.stocks = {}
        self.socket = None
        self.actions = queue.Queue()
        self.stock_lock = threading.Lock()
        self.actions_completed = queue.Queue()
        self.us = True

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
        if self.socket is None:
            if symbol not in self.stocks:
                with self.stock_lock:
                    self.stocks[symbol] = {'price': None, 'volume': None, "tracker": [instance]}
            else:
                with self.stock_lock:
                    self.stocks[symbol]["tracker"].append(instance)
        else:
            self.actions.put({"type": "subscribe", "symbol": symbol, "currency": currency, "event": event})

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

    def handle_actions(self):
        while not self.actions.empty() and self.socket is not None:
            action = self.actions.get()
            while self.actions_completed.qsize() > 3:
                time.sleep(0.05)
            if action["type"] == "subscribe":
                if self.socket is not None:
                    msg = {
                        "method": "SUBSCRIBE",
                        "params": [f"{action['symbol'].lower()}{action['currency'].lower()}@{action['event'].lower()}"],
                        "id": 1
                    }
                    self.socket.send(json.dumps(msg))
                    self.actions_completed.put(action)
            elif action["type"] == "unsubscribe":
                if self.socket is not None:
                    msg = {
                        "method": "UNSUBSCRIBE",
                        "params": [f"{action['symbol'].lower()}{action['currency'].lower()}@{action['event'].lower()}"],
                        "id": 1
                    }
                    self.socket.send(json.dumps(msg))
                    self.actions_completed.put(action)

    def rate_limit(self, limit=1):
        while self.socket is not None:
            time.sleep(limit)
            with self.actions_completed.mutex:
                self.actions_completed.queue.clear()

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
                url = f"https://data.binance.us/public_data/spot/monthly/klines/{symbol}{currency}/{interval}/{symbol}{currency}-{interval}-{month.strftime('%Y-%m')}.zip"
                r = requests.get(url)
                if r.status_code == 200:
                    with open(f"Data\\{symbol}\\{symbol}{currency}-{interval}-{month.strftime('%Y-%m')}.zip", "wb") as f:
                        f.write(r.content)
                    with zipfile.ZipFile(f"Data\\{symbol}\\{symbol}{currency}-{interval}-{month.strftime('%Y-%m')}.zip", 'r') as zip_ref:
                        zip_ref.extractall(f"Data\\{symbol}")
                    os.remove(f"Data\\{symbol}\\{symbol}{currency}-{interval}-{month.strftime('%Y-%m')}.zip")
                else:
                    continue
                pass
            if os.path.isfile(f"Data\\{symbol}\\{symbol}{currency}-{interval}-{month.strftime('%Y-%m')}.csv"):
                months_available.append(month)
        return months_available

if __name__ == "__main__":
    print([mo.strftime("%Y-%m") for mo in MarketAccess.get_history("BTC", "USD", "1m", 2)])