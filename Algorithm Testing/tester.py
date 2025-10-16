from market_access import MarketAccess
import os

class TestWallet:
    def __init__(self, initial_balance=0, currency='USD'):
        self.balance = initial_balance
        self.currency = currency
        self.coins = {}

    def deposit(self, amount):
        if amount > 0:
            self.balance += amount
            return True
        return False

    def buy(self, symbol, amount, cost):
        if self.balance >= cost:
            self.balance -= cost
            self.coins[symbol] = self.coins.get(symbol, 0) + amount
            return True
        return False

    def sell(self, symbol, amount, revenue):
        if symbol in self.coins and self.coins[symbol] >= amount:
            self.coins[symbol] -= amount
            if self.coins[symbol] == 0:
                del self.coins[symbol]
            self.balance += revenue
            return True
        return False

class BackMarket:
    def __init__ (self, market_class=MarketAccess, data_dir:str="../Data"):
        self.market = market_class
        self.available_data = self.get_available_data()
        self.data_dir = data_dir

    def get_available_data(self):
        if os.path.isdir(self.data_dir):
            coins = [f for f in os.listdir(self.data_dir) if os.path.isdir(os.path.join(self.data_dir, f))]
            data = {}
            for coin in coins:
                data[coin] = []
            return data
        else:
            return {}


class AlgTester:
    def __init__(self, market_class=MarketAccess, initial_balance=1000, currency='USD'):
        self.market_class = market_class
        self.wallet = TestWallet(initial_balance, currency)

    def backtest(self, algorithm, ):