import os
from src.simple_crypto.market_access import MarketAccess
from datetime import datetime
import pandas as pd
from typing import Literal

class BackMarket:
    def __init__ (self, market_class=MarketAccess, data_dir:str="Data"):
        self.market = market_class
        self.data_dir = data_dir
        self.available_data = MarketAccess.get_available_data(self.data_dir)

    def run_sim(self, coin:str, currency:str="USD", interval: Literal["1s", "1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "6h", "8h", "12h", "1d"] = "1m", months=1, event="miniTicker"):
        # Only does miniTicker event for now, planning to expand to more
        today = datetime.now()
        files = []
        MarketAccess.get_history(coin, currency, interval, months)
        self.available_data = MarketAccess.get_available_data(self.data_dir)
        for timespan in self.available_data[coin][currency][interval]:
            breakdown = timespan.split("-")
            year = int(breakdown[0])
            month = int(breakdown[1])
            date = datetime(year=year, month=month, day=1)
            if (today.year - year) * 12 + (today.month - month) <= months:
                files.append((date, self.available_data[coin][currency][interval][timespan]))
        files.sort(key=lambda file: file[0])
        current = None
        for file in files:
            data = pd.read_csv(file[1])
            start = 0
            if current is None:
                between = data.iloc[0, 0] - data.iloc[0, 6]
                rows_per_day = 86400000 / between
                current = data.iloc[:rows_per_day]
                frame = {}
                if event == "miniTicker":
                    frame = {
                        "e" : "24hrMiniTicker",
                        "E" : current.iloc[-1, 6],
                        "s" : (coin+currency).upper(),
                        "c" : current.iloc[-1, 4],
                        "o" : current.iloc[0, 1],
                        "h" : current['high'].max(),
                        "l" : current['low'].min(),
                        "v" : current['volume'].sum(),
                        "q" : current['quote_volume'].sum()
                    }
                yield frame
                start = rows_per_day
            for i in range(start, data.shape[0]):
                current = current.iloc[1:]
                current = current.append(data.iloc[i], ignore_index=True)
                frame = {}
                if event == "miniTicker":
                    frame = {
                        "e": "24hrMiniTicker",
                        "E": current.iloc[-1, 6],
                        "s": (coin + currency).upper(),
                        "c": current.iloc[-1, 4],
                        "o": current.iloc[0, 1],
                        "h": current['high'].max(),
                        "l": current['low'].min(),
                        "v": current['volume'].sum(),
                        "q": current['quote_volume'].sum()
                    }
                yield frame