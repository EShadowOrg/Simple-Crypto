import os
from src.simple_crypto.market_access import MarketAccess
from datetime import datetime


class BackMarket:
    def __init__ (self, market_class=MarketAccess, data_dir:str="Data"):
        self.market = market_class
        self.data_dir = data_dir
        self.available_data = self.get_available_data()

    def get_available_data(self):
        if os.path.isdir(self.data_dir):
            coins = [f for f in os.listdir(self.data_dir) if os.path.isdir(os.path.join(self.data_dir, f))]
            data = {}
            for coin in coins:
                csvs = [f for f in os.listdir(os.path.join(self.data_dir, coin)) if os.path.isfile(os.path.join(self.data_dir, coin, f)) and f.endswith('.csv')]
                data[coin] = {}
                for csv in csvs:
                    breakdown = csv.removesuffix(".csv").split("-")
                    if breakdown[1] not in data[coin]:
                        data[coin][breakdown[1]] = {}
                    if breakdown[2] not in data[coin][breakdown[1]]:
                        data[coin][breakdown[1]][breakdown[2]] = {}
                    data[coin][breakdown[1]][breakdown[2]][f"{breakdown[3]}-{breakdown[4]}"] = os.path.join(self.data_dir, coin, csv)
            return data
        else:
            return {}

    def run_sim(self, coin:str, currency:str="USD", interval:str="1m", months=1):
        today = datetime.now()
        for path in self.data[coin][currency][interval]