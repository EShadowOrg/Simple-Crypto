from src.simple_crypto.market_access import MarketAccess
from testing_wallet import TestWallet
from back_market import BackMarket
from typing import Literal


class AlgTester:
    def __init__(self, market_class=MarketAccess, wallet_class=TestWallet, initial_balance=1000, currency='USD'):
        self.market_class = market_class
        self.wallet_class = wallet_class
        self.init_balance = initial_balance
        self.currency = currency

    def backtest(self, algorithm_class, back_market_class=BackMarket, data_dir="Data", months=1, coin="BTC", interval:Literal["1s", "1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "6h", "8h", "12h", "1d"]="1m"):
        back_market = back_market_class(self.market_class, data_dir=data_dir)
        wallet = self.wallet_class(initial_balance=self.init_balance, currency=self.currency)
        algorithm = algorithm_class(market=back_market.market, wallet=wallet, coin=coin, currency=self.currency)
