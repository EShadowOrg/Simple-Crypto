from src.simple_crypto.market_access import MarketAccess
from testing_wallet import TestWallet


class AlgTester:
    def __init__(self, market_class=MarketAccess, initial_balance=1000, currency='USD'):
        self.market_class = market_class
        self.wallet = TestWallet(initial_balance, currency)

    def backtest(self, algorithm):