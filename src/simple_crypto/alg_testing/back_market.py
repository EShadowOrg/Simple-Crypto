import asyncio
import os
from src.simple_crypto.market_access import MarketAccess
from datetime import datetime
import pandas as pd
from typing import Literal
import src.simple_crypto.thread_safe_types as tst
from src.simple_crypto.misc import async_csv_to_df
from colorama import Fore

class BackMarket:

    class BackListener:
        def __init__(self, market_access:'BackMarket', coin:str, currency:str, event:str="miniTicker"):
            # Only does miniTicker event for now, planning to expand to more
            if not isinstance(event, str):
                raise TypeError("Event must be a string")
            if not isinstance(market_access, BackMarket):
                raise TypeError("market_access must be an instance of BackMarket")
            self.coin = coin
            self.currency = currency
            self.event = event
            self.market_access = market_access
            self.stopped = asyncio.Event()

        async def start(self, interval: Literal["1s", "1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "6h", "8h", "12h", "1d"] = "1m", months=1):
            identifier = f"{self.coin}{self.currency}@{self.event} for {months} months at {interval} interval"
            self.market_access.logger.log(content=f"Starting backtest listener for {identifier}", title="BACKLISTENER-START", color=Fore.GREEN)
            today = datetime.now()
            files = []
            self.market_access.logger.log(content=f"Backtest listener for {identifier} gathering historical data files...", title="BACKLISTENER-DATA", color=Fore.YELLOW)
            MarketAccess.get_history(self.coin, self.currency, interval, months)
            available_data = MarketAccess.get_available_data(self.market_access.data_dir)
            for timespan in available_data[self.coin][self.currency][interval]:
                breakdown = timespan.split("-")
                year = int(breakdown[0])
                month = int(breakdown[1])
                date = datetime(year=year, month=month, day=1)
                if (today.year - year) * 12 + (today.month - month) <= months:
                    files.append((date, available_data[self.coin][self.currency][interval][timespan]))
            self.market_access.logger.log(content=f"Backtest listener for {identifier} found {len(files)} data files to process.", title="BACKLISTENER-DATA", color=Fore.GREEN)
            files.sort(key=lambda file: file[0])
            current = None
            self.market_access.logger.log(content="Backtest listener for {identifier} beginning data playback...", title="BACKLISTENER-PLAYBACK", color=Fore.GREEN)
            for file in files:
                if self.stopped.is_set():
                    self.market_access.logger.log(content=f"Backtest listener for {identifier} stopping data playback as requested.", title="BACKLISTENER-STOP", color=Fore.RED)
                    return
                self.market_access.logger.log(content=f"Backtest listener for {identifier} processing file for {file[0].strftime('%Y-%m')}...", title="BACKLISTENER-FILE", color=Fore.YELLOW)
                data = await async_csv_to_df(os.path.join(self.market_access.data_dir, file[1]))
                start = 0
                if current is None:
                    between = data.iloc[0, 0] - data.iloc[0, 6]
                    rows_per_day = 86400000 / between
                    current = data.iloc[:rows_per_day]
                    frame = {}
                    if self.event == "miniTicker":
                        frame = {
                            "e": "24hrMiniTicker",
                            "E": current.iloc[-1, 6],
                            "s": (self.coin + self.currency).upper(),
                            "c": current.iloc[-1, 4],
                            "o": current.iloc[0, 1],
                            "h": current['high'].max(),
                            "l": current['low'].min(),
                            "v": current['volume'].sum(),
                            "q": current['quote_volume'].sum()
                        }
                    await self.market_access.msgs.put(frame)
                    await asyncio.sleep(0)
                    start = rows_per_day
                for i in range(start, data.shape[0]):
                    if self.stopped.is_set():
                        self.market_access.logger.log(content=f"Backtest listener for {identifier} stopping data playback as requested.", title="BACKLISTENER-STOP", color=Fore.RED)
                        return
                    current = current.iloc[1:]
                    current = current.append(data.iloc[i], ignore_index=True)
                    frame = {}
                    if self.event == "miniTicker":
                        frame = {
                            "e": "24hrMiniTicker",
                            "E": current.iloc[-1, 6],
                            "s": (self.coin + self.currency).upper(),
                            "c": current.iloc[-1, 4],
                            "o": current.iloc[0, 1],
                            "h": current['high'].max(),
                            "l": current['low'].min(),
                            "v": current['volume'].sum(),
                            "q": current['quote_volume'].sum()
                        }
                    await self.market_access.msgs.put(frame)
                    await asyncio.sleep(0)
            self.market_access.logger.log(content=f"Backtest listener for {identifier} completed data playback.", title="BACKLISTENER-COMPLETE", color=Fore.GREEN)

        def stop(self):
            self.stopped.set()

    def __init__ (self, market_class=MarketAccess, data_dir:str="Data", logger=None):
        self.stocks = tst.ThreadSafeStockList()
        self.msgs = asyncio.Queue(maxsize=100)
        self.market_class = market_class
        self.data_dir = data_dir
        self.listener_class = BackMarket.BackListener
        if logger is None:
            self.logger = MarketAccess.BaseLogger()
        else:
            self.logger = logger