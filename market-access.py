from typing import Literal
import os
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import pandas as pd
import zipfile
import requests
import websocket as ws

class BaseTracker:
    def __init__(self, symbol):
        self.symbol = symbol

    def on_price_update(self, price: float):
        pass

class MarketAccess:
    def __init__(self):
        self.stocks = {}
        self.socket = None

    def subscribe(self, symbol, instance, currency="USD"):
        if not isinstance(instance, BaseTracker):
            raise ValueError("Instance must be a subclass of BaseTracker")
        if not isinstance(currency, str):
            raise ValueError("Currency must be a string")
        if not isinstance(symbol, str):
            raise ValueError("Symbol must be a string")

        currency = currency.upper()
        symbol = symbol.upper()
        if self.socket is None:
            if symbol not in self.stocks:
                self.stocks[symbol] = {'price': None, 'volume': None, "tracker": [instance]}
            else:
                self.stocks[symbol]["tracker"].append(instance)
        else:
            # TODO: Implement websocket connection to get real-time data
            pass

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
            if not os.path.isfile(f"Data\\{symbol}\\{symbol}{currency}-{interval}-{month.strftime("%Y-%m")}.csv"):
                url = f"https://data.binance.us/public_data/spot/monthly/klines/{symbol}{currency}/{interval}/{symbol}{currency}-{interval}-{month.strftime("%Y-%m")}.zip"
                r = requests.get(url)
                if r.status_code == 200:
                    with open(f"Data\\{symbol}\\{symbol}{currency}-{interval}-{month.strftime("%Y-%m")}.zip", "wb") as f:
                        f.write(r.content)
                    with zipfile.ZipFile(f"Data\\{symbol}\\{symbol}{currency}-{interval}-{month.strftime("%Y-%m")}.zip", 'r') as zip_ref:
                        zip_ref.extractall(f"Data\\{symbol}")
                    os.remove(f"Data\\{symbol}\\{symbol}{currency}-{interval}-{month.strftime("%Y-%m")}.zip")
                else:
                    continue
                pass
            if os.path.isfile(f"Data\\{symbol}\\{symbol}{currency}-{interval}-{month.strftime("%Y-%m")}.csv"):
                months_available.append(month)
        return months_available

if __name__ == "__main__":
    print([mo.strftime("%Y-%m") for mo in MarketAccess.get_history("BTC", "USD", "1m", 2)])